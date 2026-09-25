use crc32fast::{Hasher, hash};

pub const MAX_PAYLOAD: usize = 1024 * 1024;
pub const RECORD_HEADER: usize = 24;
pub const SEGMENT_HEADER: usize = 32;

#[derive(Debug, PartialEq, Eq)]
pub enum Decode {
    Incomplete,
    Complete { payload: Vec<u8>, size: usize },
}

pub fn encode_record(offset: u64, payload: &[u8]) -> Result<Vec<u8>, String> {
    if payload.len() > MAX_PAYLOAD {
        return Err("payload too large".into());
    }
    let mut bytes = Vec::with_capacity(RECORD_HEADER + payload.len());
    bytes.extend(b"KLGR");
    bytes.extend(1u16.to_le_bytes());
    bytes.extend(0u16.to_le_bytes());
    bytes.extend((payload.len() as u32).to_le_bytes());
    bytes.extend(offset.to_le_bytes());
    let mut crc = Hasher::new();
    crc.update(&bytes);
    crc.update(payload);
    bytes.extend(crc.finalize().to_le_bytes());
    bytes.extend(payload);
    Ok(bytes)
}

pub fn decode_record(bytes: &[u8], expected: u64) -> Result<Decode, String> {
    if bytes.len() < RECORD_HEADER {
        return Ok(Decode::Incomplete);
    }
    if &bytes[..4] != b"KLGR"
        || u16::from_le_bytes(bytes[4..6].try_into().unwrap()) != 1
        || bytes[6..8] != [0; 2]
    {
        return Err("invalid record header or unsupported version".into());
    }
    let size = u32::from_le_bytes(bytes[8..12].try_into().unwrap()) as usize;
    if size > MAX_PAYLOAD {
        return Err("payload length exceeds limit".into());
    }
    if u64::from_le_bytes(bytes[12..20].try_into().unwrap()) != expected {
        return Err(format!("unexpected record offset, expected {expected}"));
    }
    if bytes.len() < RECORD_HEADER + size {
        return Ok(Decode::Incomplete);
    }
    let mut crc = Hasher::new();
    crc.update(&bytes[..20]);
    crc.update(&bytes[24..24 + size]);
    if crc.finalize() != u32::from_le_bytes(bytes[20..24].try_into().unwrap()) {
        return Err(format!("checksum mismatch at offset {expected}"));
    }
    Ok(Decode::Complete {
        payload: bytes[24..24 + size].to_vec(),
        size: RECORD_HEADER + size,
    })
}

pub fn segment_header(base: u64) -> [u8; SEGMENT_HEADER] {
    let mut bytes = [0; SEGMENT_HEADER];
    bytes[..4].copy_from_slice(b"KLGS");
    bytes[4..6].copy_from_slice(&1u16.to_le_bytes());
    bytes[8..16].copy_from_slice(&base.to_le_bytes());
    let crc = hash(&bytes[..28]);
    bytes[28..].copy_from_slice(&crc.to_le_bytes());
    bytes
}

pub fn decode_segment(bytes: &[u8]) -> Result<u64, String> {
    if bytes.len() != SEGMENT_HEADER
        || &bytes[..4] != b"KLGS"
        || bytes[4..6] != 1u16.to_le_bytes()
        || bytes[6..8] != [0; 2]
        || bytes[16..28] != [0; 12]
        || u32::from_le_bytes(bytes[28..32].try_into().unwrap()) != hash(&bytes[..28])
    {
        return Err("invalid segment header".into());
    }
    Ok(u64::from_le_bytes(bytes[8..16].try_into().unwrap()))
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Checkpoint {
    pub generation: u64,
    pub end: u64,
    pub base: u64,
    pub byte_end: u64,
}

impl Checkpoint {
    pub fn encode(self) -> [u8; 48] {
        let mut bytes = [0; 48];
        bytes[..4].copy_from_slice(b"KLGC");
        bytes[4..6].copy_from_slice(&1u16.to_le_bytes());
        bytes[8..16].copy_from_slice(&self.generation.to_le_bytes());
        bytes[16..24].copy_from_slice(&self.end.to_le_bytes());
        bytes[24..32].copy_from_slice(&self.base.to_le_bytes());
        bytes[32..40].copy_from_slice(&self.byte_end.to_le_bytes());
        let crc = hash(&bytes[..44]);
        bytes[44..48].copy_from_slice(&crc.to_le_bytes());
        bytes
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, String> {
        if bytes.len() != 48
            || &bytes[..4] != b"KLGC"
            || bytes[4..6] != 1u16.to_le_bytes()
            || bytes[6..8] != [0; 2]
            || bytes[40..44] != [0; 4]
            || u32::from_le_bytes(bytes[44..48].try_into().unwrap()) != hash(&bytes[..44])
        {
            return Err("invalid checkpoint".into());
        }
        Ok(Self {
            generation: u64::from_le_bytes(bytes[8..16].try_into().unwrap()),
            end: u64::from_le_bytes(bytes[16..24].try_into().unwrap()),
            base: u64::from_le_bytes(bytes[24..32].try_into().unwrap()),
            byte_end: u64::from_le_bytes(bytes[32..40].try_into().unwrap()),
        })
    }
}
