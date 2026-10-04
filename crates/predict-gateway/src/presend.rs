//! Raw HTTP/2 order path with *pre-sent* requests, the HTTP/2 analogue of the old "send all but
//! the last byte" TCP trick.
//!
//! [`H2Conn::arm`] opens a stream and sends the headers and all of the body except a held-back
//! tail, without `END_STREAM`. [`Armed::fire`] then sends only the tail (one tiny DATA frame with
//! `END_STREAM`), so on the signal the wire carries ~10–40 bytes instead of the whole request, and
//! any per-request work the proxy/origin does before reading the full body is already done.
//! Unused armed streams are dropped with [`Armed::cancel`] (`RST_STREAM`).
//!
//! Whether this actually saves time depends on whether Cloudflare forwards an unfinished request
//! to the origin; the `presend` example measures it.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use h2::client::{ResponseFuture, SendRequest};
use h2::{Reason, SendStream};
use http::header::{CONTENT_LENGTH, CONTENT_TYPE};
use http::{HeaderMap, HeaderName, HeaderValue, Method, Request, Uri};
use rustls::pki_types::ServerName;
use tokio::net::TcpStream;
use tokio_rustls::TlsConnector;

use crate::{Error, Result};

/// What is held back until [`Armed::fire`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Hold {
    /// Whole body sent up front; fire sends an empty DATA frame with `END_STREAM`.
    EndStream,
    /// Body minus its last byte; fire sends that byte with `END_STREAM`. Nobody can parse the
    /// JSON before the signal, whatever they do with `content-length`.
    LastByte,
}

/// One TLS + HTTP/2 connection driven by `h2` directly (no hyper/reqwest pool in between).
pub struct H2Conn {
    send: SendRequest<Bytes>,
    addr: SocketAddr,
    host: String,
    headers: HeaderMap,
}

/// A request whose headers and body (minus the tail) are already on the wire.
pub struct Armed {
    stream: SendStream<Bytes>,
    response: ResponseFuture,
    tail: Bytes,
}

pub struct RawResponse {
    pub status: u16,
    pub headers: HeaderMap,
    pub body: Bytes,
}

impl H2Conn {
    /// Connect to `host:443`, or to `addr` if given (to pin one edge IP), with TLS SNI `host`.
    /// `headers` are sent on every request.
    pub async fn connect(host: &str, addr: Option<SocketAddr>, headers: HeaderMap) -> Result<Self> {
        let addr = match addr {
            Some(a) => a,
            None => tokio::net::lookup_host((host, 443))
                .await?
                .find(SocketAddr::is_ipv4)
                .ok_or(Error::Transport("dns: no address".into()))?,
        };
        let tcp = TcpStream::connect(addr).await?;
        tcp.set_nodelay(true)?;

        let mut roots = rustls::RootCertStore::empty();
        roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
        let mut cfg = rustls::ClientConfig::builder_with_provider(Arc::new(
            rustls::crypto::ring::default_provider(),
        ))
        .with_safe_default_protocol_versions()
        .map_err(|e| Error::Transport(e.to_string()))?
        .with_root_certificates(roots)
        .with_no_client_auth();
        cfg.alpn_protocols = vec![b"h2".to_vec()];
        let name = ServerName::try_from(host.to_owned()).map_err(|e| Error::Transport(e.to_string()))?;
        let tls = TlsConnector::from(Arc::new(cfg)).connect(name, tcp).await?;

        let (send, conn) = h2::client::Builder::new()
            .initial_window_size(1 << 20)
            .initial_connection_window_size(1 << 22)
            .handshake::<_, Bytes>(tls)
            .await?;
        tokio::spawn(async move {
            let _ = conn.await;
        });
        Ok(Self { send: send.ready().await?, addr, host: host.to_owned(), headers })
    }

    /// Absolute URI for `path` on this connection's host. Build once, reuse per request.
    pub fn uri(&self, path: &str) -> Result<Uri> {
        Uri::try_from(format!("https://{}{path}", self.host)).map_err(|e| Error::Transport(e.to_string()))
    }

    pub fn set_header(&mut self, name: HeaderName, value: HeaderValue) {
        self.headers.insert(name, value);
    }

    /// False once the connection has failed (GOAWAY, reset, TCP drop). A connection that is merely
    /// at its concurrent-stream limit still counts as alive.
    pub async fn alive(&self) -> bool {
        !matches!(tokio::time::timeout(Duration::ZERO, self.send.clone().ready()).await, Ok(Err(_)))
    }

    /// A fresh connection to the same edge IP with the same headers.
    pub async fn reconnect(&self) -> Result<Self> {
        Self::connect(&self.host, Some(self.addr), self.headers.clone()).await
    }

    pub async fn get(&self, uri: &Uri) -> Result<RawResponse> {
        let (response, _) = self.ready().await?.send_request(self.request(Method::GET, uri), true)?;
        read(response).await
    }

    /// Plain POST: headers and whole body in one go (what reqwest does).
    pub async fn post(&self, uri: &Uri, body: Bytes) -> Result<ResponseFuture> {
        let mut req = self.request(Method::POST, uri);
        req.headers_mut().insert(CONTENT_LENGTH, HeaderValue::from(body.len()));
        let (response, mut stream) = self.ready().await?.send_request(req, false)?;
        stream.send_data(body, true)?;
        Ok(response)
    }

    /// Send headers and the body minus the held-back tail. `content_length` adds the header for
    /// the full body length.
    pub async fn arm(&self, uri: &Uri, body: &[u8], hold: Hold, content_length: bool) -> Result<Armed> {
        let split = match hold {
            Hold::EndStream => body.len(),
            Hold::LastByte => body.len().checked_sub(1).ok_or(Error::InvalidOrder("empty body"))?,
        };
        let mut req = self.request(Method::POST, uri);
        if content_length {
            req.headers_mut().insert(CONTENT_LENGTH, HeaderValue::from(body.len()));
        }
        let (response, mut stream) = self.ready().await?.send_request(req, false)?;
        stream.send_data(Bytes::copy_from_slice(&body[..split]), false)?;
        Ok(Armed { stream, response, tail: Bytes::copy_from_slice(&body[split..]) })
    }

    async fn ready(&self) -> Result<SendRequest<Bytes>> {
        Ok(self.send.clone().ready().await?)
    }

    fn request(&self, method: Method, uri: &Uri) -> Request<()> {
        let mut req = Request::new(());
        *req.method_mut() = method;
        *req.uri_mut() = uri.clone();
        *req.headers_mut() = self.headers.clone();
        req.headers_mut().insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
        req
    }
}

impl Armed {
    /// Send the held-back tail with `END_STREAM`. Returns the response future.
    pub fn fire(mut self) -> Result<ResponseFuture> {
        self.stream.send_data(self.tail, true)?;
        Ok(self.response)
    }

    /// Abort the stream (`RST_STREAM CANCEL`). The order was never complete, so it cannot execute.
    pub fn cancel(mut self) {
        self.stream.send_reset(Reason::CANCEL);
    }

    /// The response future, for detecting a server that answers before [`Armed::fire`].
    pub fn response_mut(&mut self) -> &mut ResponseFuture {
        &mut self.response
    }
}

/// Several connections, each pinned to a different edge IP, that race the *same* signed order.
/// Every copy carries the same order hash, so at most one can execute; the first to reach the
/// matching engine wins and the rest are rejected as duplicates.
pub struct Fanout {
    conns: Vec<H2Conn>,
}

/// The same request armed on every connection of a [`Fanout`].
pub struct ArmedSet(Vec<Armed>);

impl Fanout {
    /// Open `n` connections to `host`, round-robin over its IPv4 addresses.
    pub async fn connect(host: &str, n: usize, headers: HeaderMap) -> Result<Self> {
        let mut addrs: Vec<SocketAddr> =
            tokio::net::lookup_host((host, 443)).await?.filter(SocketAddr::is_ipv4).collect();
        addrs.sort();
        addrs.dedup();
        if addrs.is_empty() {
            return Err(Error::Transport("dns: no address".into()));
        }
        let mut conns = Vec::with_capacity(n);
        for i in 0..n {
            conns.push(H2Conn::connect(host, Some(addrs[i % addrs.len()]), headers.clone()).await?);
        }
        Ok(Self { conns })
    }

    pub fn conns(&self) -> &[H2Conn] {
        &self.conns
    }

    pub fn set_header(&mut self, name: HeaderName, value: HeaderValue) {
        for c in &mut self.conns {
            c.set_header(name.clone(), value.clone());
        }
    }

    /// Arm `body` on every connection (see [`H2Conn::arm`]). Connections that fail are skipped;
    /// errors only if none could be armed.
    pub async fn arm(&self, uri: &Uri, body: &[u8], hold: Hold, content_length: bool) -> Result<ArmedSet> {
        let mut set = Vec::with_capacity(self.conns.len());
        let mut last_err = None;
        for c in &self.conns {
            match c.arm(uri, body, hold, content_length).await {
                Ok(a) => set.push(a),
                Err(e) => last_err = Some(e),
            }
        }
        match last_err {
            Some(e) if set.is_empty() => Err(e),
            _ => Ok(ArmedSet(set)),
        }
    }

    /// Replace dead connections with fresh ones to the same edge IP. Returns how many were replaced.
    pub async fn heal(&mut self) -> Result<usize> {
        let mut healed = 0;
        for c in &mut self.conns {
            if !c.alive().await {
                *c = c.reconnect().await?;
                healed += 1;
            }
        }
        Ok(healed)
    }
}

impl ArmedSet {
    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Drop copies whose stream already ended before firing (server reset after holding it too
    /// long, or the connection died). Returns how many were dropped.
    pub async fn prune_dead(&mut self) -> usize {
        let before = self.0.len();
        let mut alive = Vec::with_capacity(before);
        for mut a in self.0.drain(..) {
            if tokio::time::timeout(Duration::ZERO, a.response_mut()).await.is_err() {
                alive.push(a); // still pending: healthy
            }
        }
        self.0 = alive;
        before - self.0.len()
    }

    /// Fire every copy, back to back. Responses come back in connection order.
    pub fn fire(self) -> Vec<Result<ResponseFuture>> {
        self.0.into_iter().map(Armed::fire).collect()
    }

    pub fn cancel(self) {
        self.0.into_iter().for_each(Armed::cancel);
    }
}

/// Await the response head and read the whole body.
pub async fn read(response: ResponseFuture) -> Result<RawResponse> {
    read_body(response.await?).await
}

/// Read the body of a response whose head has already arrived.
pub async fn read_body(response: http::Response<h2::RecvStream>) -> Result<RawResponse> {
    let (parts, mut body) = response.into_parts();
    let mut buf = BytesMut::new();
    while let Some(chunk) = body.data().await {
        let chunk = chunk?;
        let _ = body.flow_control().release_capacity(chunk.len());
        buf.extend_from_slice(&chunk);
    }
    Ok(RawResponse { status: parts.status.as_u16(), headers: parts.headers, body: buf.freeze() })
}
