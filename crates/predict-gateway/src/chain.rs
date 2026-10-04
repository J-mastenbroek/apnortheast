use crate::crypto::{addr, Address};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Chain {
    /// BNB Smart Chain, `api.predict.fun`. API key required.
    Mainnet,
    /// BNB testnet, `api-testnet.predict.fun`. No API key needed.
    Testnet,
}

impl Chain {
    pub const fn id(self) -> u64 {
        match self {
            Chain::Mainnet => 56,
            Chain::Testnet => 97,
        }
    }

    pub const fn api_url(self) -> &'static str {
        match self {
            Chain::Mainnet => "https://api.predict.fun",
            Chain::Testnet => "https://api-testnet.predict.fun",
        }
    }

    /// CTF exchanges, indexed by `neg_risk as usize | (yield_bearing as usize) << 1`.
    pub const fn exchanges(self) -> [Address; 4] {
        match self {
            Chain::Mainnet => [
                addr("0x8BC070BEdAB741406F4B1Eb65A72bee27894B689"),
                addr("0x365fb81bd4A24D6303cd2F19c349dE6894D8d58A"),
                addr("0x6bEb5a40C032AFc305961162d8204CDA16DECFa5"),
                addr("0x8A289d458f5a134bA40015085A8F50Ffb681B41d"),
            ],
            Chain::Testnet => [
                addr("0x2A6413639BD3d73a20ed8C95F634Ce198ABbd2d7"),
                addr("0xd690b2bd441bE36431F6F6639D7Ad351e7B29680"),
                addr("0x8a6B4Fa700A1e310b106E7a48bAFa29111f66e89"),
                addr("0x95D5113bc50eD201e319101bbca3e0E250662fCC"),
            ],
        }
    }
}

pub(crate) const fn exchange_index(neg_risk: bool, yield_bearing: bool) -> usize {
    neg_risk as usize | (yield_bearing as usize) << 1
}
