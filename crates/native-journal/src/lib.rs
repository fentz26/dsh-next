//! Native bounded byte journal — Phase 0 pilot for dsh-next.
//!
//! Semantics contract (mirrors DSH OutputCollector / packages/journal):
//! - append-only stream of bytes with absolute whole-stream offsets
//! - bounded tail retention (`maxBytes`); an incoming chunk may transiently
//!   exceed the cap before head-trimming back to byte-exact bounds
//! - readFrom(offset) returns retained suffix since `offset`; non-consuming;
//!   `lossy` when offset slid below the window start
//!
//! Memory ownership:
//! - append: JS-owned Buffer is COPIED into Rust-owned storage exactly once
//!   at the N-API boundary; the journal owns those bytes until eviction.
//! - readFrom: retained bytes are assembled and copied out into one fresh
//!   allocation exposed to JS as a Buffer; the journal retains its own copy
//!   (reads are non-consuming, independent readers are safe).
//! - Errors are Result<> mapped to JS exceptions; this crate never panics on
//!   ordinary bad input. max_bytes guards integer overflow internally.

use std::collections::VecDeque;

use napi::bindgen_prelude::Buffer;
use napi_derive::napi;

struct Segment {
    /// Absolute stream offset of data[0].
    start: u64,
    data: Vec<u8>,
}

#[napi(object)]
pub struct JournalRead {
    pub data: Buffer,
    pub next_offset: f64,
    pub lossy: bool,
}

#[napi]
pub struct NativeByteJournal {
    max_bytes: u64,
    /// Absolute stream offset of the first retained byte.
    window_start: u64,
    total: u64,
    segments: VecDeque<Segment>,
}

#[napi]
impl NativeByteJournal {
    #[napi(constructor)]
    pub fn new(max_bytes: f64) -> napi::Result<Self> {
        if !max_bytes.is_finite() || max_bytes <= 0.0 || max_bytes.fract() != 0.0 {
            return Err(napi::Error::new(
                napi::Status::InvalidArg,
                "maxBytes must be a positive integer".to_owned(),
            ));
        }
        Ok(Self {
            max_bytes: max_bytes as u64,
            window_start: 0,
            total: 0,
            segments: VecDeque::new(),
        })
    }

    #[napi(getter)]
    pub fn next_offset(&self) -> f64 {
        self.total as f64
    }

    #[napi(getter)]
    pub fn window_start(&self) -> f64 {
        self.window_start as f64
    }

    /// Append bytes; copies once across the boundary; returns the new total.
    #[napi]
    pub fn append(&mut self, chunk: Buffer) -> napi::Result<f64> {
        let len = chunk.len();
        let data = Vec::from(chunk.as_ref());
        self.total += len as u64;
        self.segments.push_back(Segment {
            start: self.total - len as u64,
            data,
        });
        self.evict();
        Ok(self.total as f64)
    }

    /// Append many buffers in one FFI crossing (batched boundary).
    #[napi]
    pub fn append_batch(&mut self, chunks: Vec<Buffer>) -> napi::Result<f64> {
        for chunk in chunks {
            let len = chunk.len();
            let data = Vec::from(chunk.as_ref());
            self.total += len as u64;
            self.segments.push_back(Segment {
                start: self.total - len as u64,
                data,
            });
        }
        self.evict();
        Ok(self.total as f64)
    }

    /// Read retained bytes since `offset`. Non-consuming, independent readers.
    #[napi]
    pub fn read_from(&self, offset: f64) -> napi::Result<JournalRead> {
        if !offset.is_finite() || offset < 0.0 {
            return Err(napi::Error::new(
                napi::Status::InvalidArg,
                "offset must be a non-negative finite number".to_owned(),
            ));
        }
        let lossy = offset < self.window_start as f64;
        let from = if lossy { self.window_start } else { offset as u64 };

        let mut out = Vec::new();
        for seg in &self.segments {
            let seg_end = seg.start + seg.data.len() as u64;
            if seg_end <= from {
                continue;
            }
            let skip = if seg.start < from {
                (from - seg.start) as usize
            } else {
                0
            };
            out.extend_from_slice(&seg.data[skip..]);
        }

        Ok(JournalRead {
            data: out.into(),
            next_offset: self.total as f64,
            lossy,
        })
    }

    fn evict(&mut self) {
        let retained = self.total - self.window_start;
        let mut excess = retained.saturating_sub(self.max_bytes);
        while excess > 0 {
            match self.segments.front_mut() {
                Some(head) => {
                    let head_len = head.data.len() as u64;
                    if head_len <= excess {
                        excess -= head_len;
                        self.window_start += head_len;
                        self.segments.pop_front();
                    } else {
                        head.data.drain(..excess as usize);
                        head.start += excess;
                        self.window_start += excess;
                        excess = 0;
                    }
                }
                None => break,
            }
        }
    }
}

/// Probe used by diagnostics: proves the native module loaded and runs.
#[napi]
pub fn probe() -> String {
    "dsh-next-native-journal ok".to_owned()
}
