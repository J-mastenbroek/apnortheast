#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("http: {0}")]
    Http(#[from] reqwest::Error),
    #[error("api {status} {code}: {message}")]
    Api { status: u16, code: String, message: String },
    #[error("decode: {0}")]
    Decode(#[from] serde_json::Error),
    #[error("invalid order: {0}")]
    InvalidOrder(&'static str),
    #[error("invalid private key")]
    InvalidKey,
    #[error("invalid address")]
    InvalidAddress,
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("h2: {0}")]
    H2(#[from] h2::Error),
    #[error("transport: {0}")]
    Transport(String),
    #[error("missing env var {0}")]
    MissingEnv(&'static str),
}

pub type Result<T, E = Error> = std::result::Result<T, E>;
