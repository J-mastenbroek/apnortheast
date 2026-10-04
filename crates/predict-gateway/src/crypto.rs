//! Keccak, secp256k1 signing and EIP-712 helpers. Everything here is synchronous and allocation-free.

use secp256k1::{Message, PublicKey, Secp256k1, SecretKey, SignOnly};
use tiny_keccak::{Hasher, Keccak};

use crate::Error;

pub type Address = [u8; 20];
pub type B256 = [u8; 32];

#[inline]
pub fn keccak256(data: &[u8]) -> B256 {
    let mut k = Keccak::v256();
    k.update(data);
    let mut out = [0u8; 32];
    k.finalize(&mut out);
    out
}

/// EIP-191 `personal_sign` digest.
pub fn eip191_hash(msg: &[u8]) -> B256 {
    let mut k = Keccak::v256();
    k.update(b"\x19Ethereum Signed Message:\n");
    k.update(msg.len().to_string().as_bytes());
    k.update(msg);
    let mut out = [0u8; 32];
    k.finalize(&mut out);
    out
}

/// `keccak256(0x1901 ‖ domain_separator ‖ struct_hash)`.
#[inline]
pub fn eip712_digest(domain_separator: &B256, struct_hash: &B256) -> B256 {
    let mut buf = [0u8; 66];
    buf[0] = 0x19;
    buf[1] = 0x01;
    buf[2..34].copy_from_slice(domain_separator);
    buf[34..].copy_from_slice(struct_hash);
    keccak256(&buf)
}

pub fn domain_separator(name: &str, version: &str, chain_id: u64, contract: &Address) -> B256 {
    let mut buf = [0u8; 160];
    buf[0..32].copy_from_slice(&keccak256(
        b"EIP712Domain(string name,string version,uint256 chainId,address verifyingContract)",
    ));
    buf[32..64].copy_from_slice(&keccak256(name.as_bytes()));
    buf[64..96].copy_from_slice(&keccak256(version.as_bytes()));
    buf[120..128].copy_from_slice(&chain_id.to_be_bytes());
    buf[140..160].copy_from_slice(contract);
    keccak256(&buf)
}

pub struct Signer {
    secp: Secp256k1<SignOnly>,
    key: SecretKey,
    address: Address,
}

impl Signer {
    pub fn from_hex(hex: &str) -> Result<Self, Error> {
        let bytes = decode_hex(hex).ok_or(Error::InvalidKey)?;
        let secp = Secp256k1::signing_only();
        let key = SecretKey::from_slice(&bytes).map_err(|_| Error::InvalidKey)?;
        let pubkey = PublicKey::from_secret_key(&secp, &key).serialize_uncompressed();
        let mut address = [0u8; 20];
        address.copy_from_slice(&keccak256(&pubkey[1..])[12..]);
        Ok(Self { secp, key, address })
    }

    pub fn address(&self) -> Address {
        self.address
    }

    /// 65-byte `r ‖ s ‖ v` signature over a 32-byte digest, v ∈ {27, 28}.
    #[inline]
    pub fn sign_hash(&self, digest: &B256) -> [u8; 65] {
        let sig = self
            .secp
            .sign_ecdsa_recoverable(&Message::from_digest(*digest), &self.key);
        let (rec_id, rs) = sig.serialize_compact();
        let mut out = [0u8; 65];
        out[..64].copy_from_slice(&rs);
        out[64] = 27 + rec_id.to_i32() as u8;
        out
    }
}

/// Signature as sent to the API: 65 bytes for an EOA, 86 bytes for a Kernel smart account.
pub struct Signature {
    bytes: [u8; 86],
    len: usize,
}

impl Signature {
    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes[..self.len]
    }
}

pub const KERNEL_ECDSA_VALIDATOR: Address = addr("0x845ADb2C711129d4f3966735eD98a9F09fC4cE57");

/// How the maker signs: directly with the key, or via a predict.fun smart account (ZeroDev Kernel 0.3.1).
pub(crate) enum SignMode {
    Eoa,
    Kernel { domain_separator: B256 },
}

impl SignMode {
    pub fn kernel(chain_id: u64, account: &Address) -> Self {
        Self::Kernel { domain_separator: domain_separator("Kernel", "0.3.1", chain_id, account) }
    }

    /// Sign `hash` (an EIP-191 or EIP-712 digest) as the maker.
    #[inline]
    pub fn sign(&self, signer: &Signer, hash: &B256) -> Signature {
        let mut bytes = [0u8; 86];
        match self {
            SignMode::Eoa => {
                bytes[..65].copy_from_slice(&signer.sign_hash(hash));
                Signature { bytes, len: 65 }
            }
            SignMode::Kernel { domain_separator } => {
                let mut buf = [0u8; 64];
                buf[..32].copy_from_slice(&keccak256(b"Kernel(bytes32 hash)"));
                buf[32..].copy_from_slice(hash);
                let digest = eip712_digest(domain_separator, &keccak256(&buf));
                bytes[0] = 0x01;
                bytes[1..21].copy_from_slice(&KERNEL_ECDSA_VALIDATOR);
                bytes[21..].copy_from_slice(&signer.sign_hash(&eip191_hash(&digest)));
                Signature { bytes, len: 86 }
            }
        }
    }
}

// ---- encoding helpers ----

const HEX: &[u8; 16] = b"0123456789abcdef";

/// Append `0x`-prefixed lowercase hex.
#[inline]
pub fn push_hex(out: &mut String, bytes: &[u8]) {
    out.push_str("0x");
    for b in bytes {
        out.push(HEX[(b >> 4) as usize] as char);
        out.push(HEX[(b & 0xf) as usize] as char);
    }
}

pub fn to_hex(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(2 + bytes.len() * 2);
    push_hex(&mut s, bytes);
    s
}

pub fn decode_hex(s: &str) -> Option<Vec<u8>> {
    let s = s.trim().strip_prefix("0x").unwrap_or(s.trim()).as_bytes();
    if s.len() % 2 != 0 {
        return None;
    }
    s.chunks(2)
        .map(|p| Some((nibble(p[0])? << 4) | nibble(p[1])?))
        .collect()
}

pub fn parse_address(s: &str) -> Result<Address, Error> {
    decode_hex(s)
        .and_then(|v| v.try_into().ok())
        .ok_or(Error::InvalidAddress)
}

const fn nibble(c: u8) -> Option<u8> {
    match c {
        b'0'..=b'9' => Some(c - b'0'),
        b'a'..=b'f' => Some(c - b'a' + 10),
        b'A'..=b'F' => Some(c - b'A' + 10),
        _ => None,
    }
}

/// Compile-time address literal.
pub const fn addr(s: &str) -> Address {
    let b = s.as_bytes();
    assert!(b.len() == 42);
    let mut out = [0u8; 20];
    let mut i = 0;
    while i < 20 {
        let (Some(hi), Some(lo)) = (nibble(b[2 + 2 * i]), nibble(b[3 + 2 * i])) else {
            panic!("bad hex in address")
        };
        out[i] = (hi << 4) | lo;
        i += 1;
    }
    out
}

/// Parse a decimal uint256 into a big-endian 32-byte word.
pub fn parse_u256_dec(s: &str) -> Option<B256> {
    let mut limbs = [0u64; 4]; // little-endian
    if s.is_empty() {
        return None;
    }
    for c in s.bytes() {
        let d = c.checked_sub(b'0').filter(|d| *d < 10)? as u128;
        let mut carry = d;
        for limb in &mut limbs {
            let v = (*limb as u128) * 10 + carry;
            *limb = v as u64;
            carry = v >> 64;
        }
        if carry != 0 {
            return None;
        }
    }
    let mut out = [0u8; 32];
    for (i, limb) in limbs.iter().rev().enumerate() {
        out[i * 8..i * 8 + 8].copy_from_slice(&limb.to_be_bytes());
    }
    Some(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn address_from_key() {
        // Well-known hardhat account #0.
        let s = Signer::from_hex("0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80").unwrap();
        assert_eq!(to_hex(&s.address()), "0xf39fd6e51aad88f6f4ce6ab8827279cfffb92266");
    }

    #[test]
    fn u256_decimal() {
        let w = parse_u256_dec("256").unwrap();
        assert_eq!(&w[30..], &[1, 0]);
        let max = "115792089237316195423570985008687907853269984665640564039457584007913129639935";
        assert_eq!(parse_u256_dec(max).unwrap(), [0xff; 32]);
        assert!(parse_u256_dec("115792089237316195423570985008687907853269984665640564039457584007913129639936").is_none());
    }
}
