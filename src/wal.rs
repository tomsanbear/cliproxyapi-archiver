//! Decoder for the archive WAL frame and record format, `docs/archive-wal-format.md` sections 5–7.
//!
//! The walking skeleton reads one whole segment into memory. Segment discovery, draining and
//! deletion belong to the `wal-reader` ticket; the damage rules of section 6 are implemented here
//! because they are a few lines each and the fixtures exercise all of them.

use std::path::{Path, PathBuf};

use anyhow::{Context, Result};

/// ASCII `CPAW`, the first four bytes of every frame header.
pub const MAGIC: [u8; 4] = *b"CPAW";
/// The only frame version this reader decodes.
pub const VERSION: u8 = 1;
/// Every frame header is 20 bytes, in every version.
pub const HEADER_LEN: usize = 20;
/// Largest `payload_len` a writer produces; a larger value means the header is corrupt.
pub const MAX_PAYLOAD_LEN: u32 = 128 << 20;
/// Largest `record_len` a writer produces; a larger value makes the frame a bad record.
pub const MAX_RECORD_LEN: u32 = 256 << 20;

const FLAG_ZSTD: u8 = 0b1;

/// One decoded record: the three sections of section 7, as stored.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Record {
    /// UTF-8 JSON meta object (section 7.1), not yet parsed.
    pub meta: Vec<u8>,
    /// Inbound request body, exact bytes after transport decoding.
    pub request: Vec<u8>,
    /// Response bytes exactly as delivered to the client.
    pub response: Vec<u8>,
}

/// Counts of what happened while reading a segment, named as in section 6 and `manifest.json`.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct ReadStats {
    /// Records decoded and yielded.
    pub frames: u64,
    pub unknown_version: u64,
    pub crc_mismatch: u64,
    pub bad_record: u64,
    /// The segment ended with a partial header or payload.
    pub truncated_tail: bool,
    /// Reading stopped at a header that cannot be trusted (bad magic or oversized payload).
    pub corrupt: bool,
}

/// A decoded segment: its valid records in file order plus the read stats.
#[derive(Debug, Default)]
pub struct Segment {
    pub records: Vec<Record>,
    pub stats: ReadStats,
}

/// `seg-<seq>.wal`, with `seq` zero-padded to 12 digits (section 1).
pub fn segment_file_name(seq: u64) -> String {
    format!("seg-{seq:012}.wal")
}

/// Path of completed segment `seq` in `spool`. Never names a `.wal.open` file.
pub fn segment_path(spool: &Path, seq: u64) -> PathBuf {
    spool.join(segment_file_name(seq))
}

/// Read and decode a completed segment file.
pub fn read_segment(path: &Path) -> Result<Segment> {
    let bytes =
        std::fs::read(path).with_context(|| format!("reading segment {}", path.display()))?;
    Ok(decode_segment(&bytes))
}

/// Decode a whole segment per section 6. Damage never fails the call; it is counted in the stats.
pub fn decode_segment(bytes: &[u8]) -> Segment {
    let mut segment = Segment::default();
    let stats = &mut segment.stats;
    let mut rest = bytes;

    loop {
        // Step 1: a partial header is a truncated tail; an empty remainder is a clean end.
        if rest.len() < HEADER_LEN {
            stats.truncated_tail = !rest.is_empty();
            break;
        }
        let header = Header::parse(&rest[..HEADER_LEN]);
        // Step 2: once the header cannot be trusted there is no length to skip by.
        if header.magic != MAGIC || header.payload_len > MAX_PAYLOAD_LEN {
            stats.corrupt = true;
            break;
        }
        let body = &rest[HEADER_LEN..];
        let payload_len = header.payload_len as usize;
        // Step 4 (checked before step 3 so an unknown-version frame cut short is a truncated
        // tail rather than a skip past the end of the file).
        if body.len() < payload_len {
            stats.truncated_tail = true;
            break;
        }
        let (payload, next) = body.split_at(payload_len);
        rest = next;

        // Step 3.
        if header.version != VERSION {
            stats.unknown_version += 1;
            continue;
        }
        // Step 5.
        if crc32c::crc32c(payload) != header.crc32c {
            stats.crc_mismatch += 1;
            continue;
        }
        // Steps 6 and 7.
        match decode_payload(&header, payload).and_then(|record| unpack_record(&record)) {
            Some(record) => {
                stats.frames += 1;
                segment.records.push(record);
            }
            None => stats.bad_record += 1,
        }
    }
    segment
}

struct Header {
    magic: [u8; 4],
    version: u8,
    flags: u8,
    payload_len: u32,
    record_len: u32,
    crc32c: u32,
}

impl Header {
    fn parse(b: &[u8]) -> Self {
        let u32_at = |at: usize| u32::from_le_bytes([b[at], b[at + 1], b[at + 2], b[at + 3]]);
        Self {
            magic: [b[0], b[1], b[2], b[3]],
            version: b[4],
            flags: b[5],
            // Bytes 6..8 are reserved and opaque.
            payload_len: u32_at(8),
            record_len: u32_at(12),
            crc32c: u32_at(16),
        }
    }
}

/// Step 6: decompress per `flags` and check the length. `None` is a bad record.
fn decode_payload(header: &Header, payload: &[u8]) -> Option<Vec<u8>> {
    if header.record_len > MAX_RECORD_LEN {
        return None;
    }
    let record_len = header.record_len as usize;
    let record = if header.flags & FLAG_ZSTD != 0 {
        // The capacity bound makes an over-long frame fail instead of growing without limit;
        // the zstd content checksum is verified by the decoder.
        zstd::bulk::decompress(payload, record_len).ok()?
    } else {
        payload.to_vec()
    };
    (record.len() == record_len).then_some(record)
}

/// Step 7: split the record into its three length-prefixed sections, which must fill it exactly.
fn unpack_record(record: &[u8]) -> Option<Record> {
    fn section<'a>(rest: &mut &'a [u8]) -> Option<&'a [u8]> {
        let (len, tail) = rest.split_first_chunk::<4>()?;
        let len = u32::from_le_bytes(*len) as usize;
        if tail.len() < len {
            return None;
        }
        let (section, tail) = tail.split_at(len);
        *rest = tail;
        Some(section)
    }

    let mut rest = record;
    let meta = section(&mut rest)?;
    let request = section(&mut rest)?;
    let response = section(&mut rest)?;
    rest.is_empty().then(|| Record {
        meta: meta.to_vec(),
        request: request.to_vec(),
        response: response.to_vec(),
    })
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::path::PathBuf;

    use serde::Deserialize;
    use sha2::{Digest, Sha256};

    use super::*;

    #[derive(Deserialize)]
    struct Manifest {
        v: u32,
        segments: BTreeMap<String, ManifestSegment>,
        ignored: Vec<String>,
    }

    #[derive(Deserialize)]
    struct ManifestSegment {
        stats: ManifestStats,
        records: Vec<ManifestRecord>,
    }

    #[derive(Deserialize, Debug, PartialEq, Eq)]
    struct ManifestStats {
        frames: u64,
        unknown_version: u64,
        crc_mismatch: u64,
        bad_record: u64,
        truncated_tail: bool,
        corrupt: bool,
    }

    #[derive(Deserialize, Debug, PartialEq, Eq)]
    struct ManifestRecord {
        request_id: String,
        attempt: u64,
        format: String,
        meta_sha256: String,
        request_sha256: String,
        response_sha256: String,
    }

    #[derive(Deserialize)]
    struct MetaKeys {
        request_id: String,
        attempt: u64,
        request: MetaRequest,
    }

    #[derive(Deserialize)]
    struct MetaRequest {
        format: String,
    }

    fn fixtures_dir() -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("fixtures/wal")
    }

    fn sha256_hex(bytes: &[u8]) -> String {
        Sha256::digest(bytes)
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect()
    }

    fn manifest() -> Manifest {
        let raw = std::fs::read(fixtures_dir().join("manifest.json")).unwrap();
        let manifest: Manifest = serde_json::from_slice(&raw).unwrap();
        assert_eq!(manifest.v, 1);
        manifest
    }

    fn observed(segment: &Segment) -> (ManifestStats, Vec<ManifestRecord>) {
        let s = &segment.stats;
        let stats = ManifestStats {
            frames: s.frames,
            unknown_version: s.unknown_version,
            crc_mismatch: s.crc_mismatch,
            bad_record: s.bad_record,
            truncated_tail: s.truncated_tail,
            corrupt: s.corrupt,
        };
        let records = segment
            .records
            .iter()
            .map(|r| {
                let keys: MetaKeys = serde_json::from_slice(&r.meta).unwrap();
                ManifestRecord {
                    request_id: keys.request_id,
                    attempt: keys.attempt,
                    format: keys.request.format,
                    meta_sha256: sha256_hex(&r.meta),
                    request_sha256: sha256_hex(&r.request),
                    response_sha256: sha256_hex(&r.response),
                }
            })
            .collect();
        (stats, records)
    }

    #[test]
    fn reproduces_manifest_for_every_segment() {
        let manifest = manifest();
        assert!(manifest.segments.contains_key("seg-000000000001.wal"));
        for (name, expected) in &manifest.segments {
            let segment = read_segment(&fixtures_dir().join(name)).unwrap();
            let (stats, records) = observed(&segment);
            assert_eq!(stats, expected.stats, "stats for {name}");
            assert_eq!(records, expected.records, "records for {name}");
        }
    }

    #[test]
    fn manifest_covers_every_completed_segment_and_ignores_open_ones() {
        let manifest = manifest();
        let mut completed = Vec::new();
        let mut other = Vec::new();
        for entry in std::fs::read_dir(fixtures_dir()).unwrap() {
            let name = entry.unwrap().file_name().into_string().unwrap();
            if name.starts_with("seg-") && name.ends_with(".wal") {
                completed.push(name);
            } else if name != "manifest.json" {
                other.push(name);
            }
        }
        completed.sort();
        other.sort();
        assert_eq!(
            completed,
            manifest.segments.keys().cloned().collect::<Vec<_>>()
        );
        assert_eq!(other, manifest.ignored);
    }

    #[test]
    fn segment_names_are_zero_padded() {
        assert_eq!(segment_file_name(1), "seg-000000000001.wal");
        assert_eq!(
            segment_path(Path::new("/spool"), 123),
            PathBuf::from("/spool/seg-000000000123.wal")
        );
    }

    #[test]
    fn short_garbage_is_a_truncated_tail_and_empty_input_is_clean() {
        let segment = decode_segment(b"CPAW\x01");
        assert!(segment.stats.truncated_tail && !segment.stats.corrupt);
        let segment = decode_segment(b"");
        assert_eq!(segment.stats, ReadStats::default());
    }

    #[test]
    fn uncompressed_frames_decode() {
        let mut record = Vec::new();
        for section in [&b"{}"[..], b"req", b""] {
            record.extend_from_slice(&(section.len() as u32).to_le_bytes());
            record.extend_from_slice(section);
        }
        let mut frame = Vec::new();
        frame.extend_from_slice(&MAGIC);
        frame.extend_from_slice(&[VERSION, 0b1000_0000, 0, 0]); // unknown flag bits are ignored
        frame.extend_from_slice(&(record.len() as u32).to_le_bytes());
        frame.extend_from_slice(&(record.len() as u32).to_le_bytes());
        frame.extend_from_slice(&crc32c::crc32c(&record).to_le_bytes());
        frame.extend_from_slice(&record);

        let segment = decode_segment(&frame);
        assert_eq!(segment.stats.frames, 1);
        assert_eq!(
            segment.records,
            vec![Record {
                meta: b"{}".to_vec(),
                request: b"req".to_vec(),
                response: Vec::new(),
            }]
        );
    }
}
