use crate::format::{
    Checkpoint, Decode, MAX_PAYLOAD, RECORD_HEADER, SEGMENT_HEADER, decode_record, decode_segment,
    encode_record, segment_header,
};
use std::{
    fs::{self, File, OpenOptions},
    io::{self, Write},
    os::unix::fs::{DirBuilderExt, FileExt},
    path::{Path, PathBuf},
};

const DEFAULT_SEGMENT_LIMIT: u64 = 64 * 1024 * 1024;
const MAX_SEGMENTS: usize = 64;

fn invalid(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message.into())
}

fn name(base: u64) -> String {
    format!("{base:020}.log")
}

fn lock(dir: &Path) -> io::Result<File> {
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(dir.join("store.lock"))?;
    file.try_lock()?;
    Ok(file)
}

fn sync_dir(dir: &Path) -> io::Result<()> {
    File::open(dir)?.sync_all()
}

fn save_checkpoint(dir: &Path, checkpoint: Checkpoint) -> io::Result<()> {
    let mut temp = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(dir.join("checkpoint.tmp"))?;
    temp.write_all(&checkpoint.encode())?;
    temp.sync_all()?;
    fs::rename(dir.join("checkpoint.tmp"), dir.join("checkpoint"))?;
    sync_dir(dir)
}

struct Segment {
    base: u64,
    end: u64,
    byte_end: u64,
    index: Vec<(u64, u64)>,
}

pub struct Page {
    pub end: u64,
    pub next: u64,
    pub records: Vec<Vec<u8>>,
}

pub struct Store {
    dir: PathBuf,
    _lock: File,
    log: File,
    segments: Vec<Segment>,
    checkpoint: Checkpoint,
    segment_limit: u64,
    failed: bool,
}

fn record_at(
    file: &File,
    position: u64,
    boundary: u64,
    expected: u64,
) -> io::Result<Option<(Vec<u8>, usize)>> {
    if boundary - position < RECORD_HEADER as u64 {
        return Ok(None);
    }
    let mut head = [0; RECORD_HEADER];
    file.read_exact_at(&mut head, position)?;
    if &head[..4] != b"KLGR" || head[4..6] != 1u16.to_le_bytes() || head[6..8] != [0; 2] {
        return Err(invalid(format!("invalid record header at byte {position}")));
    }
    let length = u32::from_le_bytes(head[8..12].try_into().unwrap()) as usize;
    if length > MAX_PAYLOAD {
        return Err(invalid(format!("oversized record at byte {position}")));
    }
    let size = RECORD_HEADER + length;
    if boundary - position < size as u64 {
        return Ok(None);
    }
    let mut bytes = vec![0; size];
    file.read_exact_at(&mut bytes, position)?;
    match decode_record(&bytes, expected).map_err(invalid)? {
        Decode::Complete { .. } => Ok(Some((bytes, size))),
        Decode::Incomplete => Err(invalid("incomplete complete-length record")),
    }
}

impl Store {
    pub fn init(dir: &Path) -> io::Result<Self> {
        Self::init_with_limit(dir, DEFAULT_SEGMENT_LIMIT)
    }

    pub fn init_with_limit(dir: &Path, segment_limit: u64) -> io::Result<Self> {
        if segment_limit < SEGMENT_HEADER as u64 + RECORD_HEADER as u64
            || segment_limit > DEFAULT_SEGMENT_LIMIT
        {
            return Err(invalid("invalid segment limit"));
        }
        let mut builder = fs::DirBuilder::new();
        builder.mode(0o700).create(dir)?;
        sync_dir(dir.parent().ok_or_else(|| invalid("missing parent"))?)?;
        OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(dir.join("store.lock"))?
            .sync_all()?;
        let guard = lock(dir)?;
        let mut log = OpenOptions::new()
            .create_new(true)
            .read(true)
            .write(true)
            .open(dir.join(name(0)))?;
        log.write_all(&segment_header(0))?;
        log.sync_all()?;
        sync_dir(dir)?;
        let checkpoint = Checkpoint {
            generation: 0,
            end: 0,
            base: 0,
            byte_end: SEGMENT_HEADER as u64,
        };
        save_checkpoint(dir, checkpoint)?;
        Ok(Self {
            dir: dir.into(),
            _lock: guard,
            log,
            segments: vec![Segment {
                base: 0,
                end: 0,
                byte_end: SEGMENT_HEADER as u64,
                index: vec![],
            }],
            checkpoint,
            segment_limit,
            failed: false,
        })
    }

    pub fn open(dir: &Path) -> io::Result<Self> {
        Self::open_with_limit(dir, DEFAULT_SEGMENT_LIMIT)
    }

    pub fn open_with_limit(dir: &Path, segment_limit: u64) -> io::Result<Self> {
        if segment_limit < SEGMENT_HEADER as u64 + RECORD_HEADER as u64
            || segment_limit > DEFAULT_SEGMENT_LIMIT
        {
            return Err(invalid("invalid segment limit"));
        }
        let guard = lock(dir)?;
        let checkpoint = Checkpoint::decode(&fs::read(dir.join("checkpoint"))?).map_err(invalid)?;
        if checkpoint.byte_end < SEGMENT_HEADER as u64 {
            return Err(invalid("checkpoint byte end before header"));
        }
        let mut bases = Vec::new();
        let mut temporary = Vec::new();
        for entry in fs::read_dir(dir)? {
            let entry = entry?;
            let file_name = entry.file_name().to_string_lossy().into_owned();
            if file_name == "checkpoint" || file_name == "store.lock" {
                continue;
            }
            if file_name == "checkpoint.tmp"
                || (file_name.len() == 24
                    && file_name.ends_with(".tmp")
                    && file_name[..20].bytes().all(|b| b.is_ascii_digit()))
            {
                temporary.push(entry.path());
                continue;
            }
            if file_name.len() != 24
                || !file_name.ends_with(".log")
                || !file_name[..20].bytes().all(|b| b.is_ascii_digit())
            {
                return Err(invalid(format!("unexpected store entry: {file_name}")));
            }
            let base = file_name[..20]
                .parse::<u64>()
                .map_err(|_| invalid("invalid segment filename"))?;
            if name(base) != file_name {
                return Err(invalid("noncanonical segment name"));
            }
            bases.push(base);
        }
        bases.sort_unstable();
        if bases.first() != Some(&0) || bases.len() > MAX_SEGMENTS {
            return Err(invalid("missing first segment or too many segments"));
        }
        let mut segments = Vec::new();
        let mut next = 0u64;
        let mut protected = false;
        for (i, &base) in bases.iter().enumerate() {
            if base != next {
                return Err(invalid(format!(
                    "segment gap or duplicate at {base}, expected {next}"
                )));
            }
            let file = OpenOptions::new()
                .read(true)
                .write(true)
                .open(dir.join(name(base)))?;
            let length = file.metadata()?.len();
            if length < SEGMENT_HEADER as u64 || length > segment_limit {
                return Err(invalid("segment length invalid"));
            }
            let mut head = [0; SEGMENT_HEADER];
            file.read_exact_at(&mut head, 0)?;
            if decode_segment(&head).map_err(invalid)? != base {
                return Err(invalid("segment header base mismatch"));
            }
            let mut position = SEGMENT_HEADER as u64;
            let mut index = Vec::new();
            while position < length {
                let record = record_at(&file, position, length, next)?;
                let Some((_, size)) = record else {
                    if i + 1 != bases.len()
                        || base != checkpoint.base
                        || position < checkpoint.byte_end
                    {
                        return Err(invalid("incomplete protected or sealed record"));
                    }
                    break;
                };
                if (next - base) % 64 == 0 {
                    index.push((next, position));
                }
                next = next
                    .checked_add(1)
                    .ok_or_else(|| invalid("offset overflow"))?;
                position += size as u64;
                if base == checkpoint.base
                    && position == checkpoint.byte_end
                    && next == checkpoint.end
                {
                    protected = true;
                }
                if base == checkpoint.base && position > checkpoint.byte_end && !protected {
                    return Err(invalid("checkpoint is not a record boundary"));
                }
            }
            if base == checkpoint.base && position == checkpoint.byte_end && next == checkpoint.end
            {
                protected = true;
            }
            if i + 1 != bases.len() && position == SEGMENT_HEADER as u64 {
                return Err(invalid("empty predecessor segment"));
            }
            segments.push(Segment {
                base,
                end: next,
                byte_end: position,
                index,
            });
        }
        if !protected {
            return Err(invalid("checkpoint does not match history"));
        }
        let last = segments.last().unwrap();
        if checkpoint.base != last.base
            && !(bases.len() >= 2
                && checkpoint.base == bases[bases.len() - 2]
                && last.byte_end == SEGMENT_HEADER as u64
                && checkpoint.end == last.base)
        {
            return Err(invalid("unexpected successor after checkpoint"));
        }
        let last_file = OpenOptions::new()
            .write(true)
            .open(dir.join(name(last.base)))?;
        if last_file.metadata()?.len() > last.byte_end {
            last_file.set_len(last.byte_end)?;
            last_file.sync_all()?;
        }
        let changed = next != checkpoint.end
            || last.base != checkpoint.base
            || last.byte_end != checkpoint.byte_end;
        let mut current = checkpoint;
        if changed {
            if last.byte_end < checkpoint.byte_end && last.base == checkpoint.base {
                return Err(invalid("protected bytes lost"));
            }
            file_sync_last(dir, last.base)?;
            current.generation = current
                .generation
                .checked_add(1)
                .ok_or_else(|| invalid("generation overflow"))?;
            current.end = next;
            current.base = last.base;
            current.byte_end = last.byte_end;
        }
        for path in temporary {
            fs::remove_file(path)?;
        }
        if changed {
            save_checkpoint(dir, current)?;
        } else {
            sync_dir(dir)?;
        }
        let log = OpenOptions::new()
            .read(true)
            .write(true)
            .open(dir.join(name(last.base)))?;
        Ok(Self {
            dir: dir.into(),
            _lock: guard,
            log,
            segments,
            checkpoint: current,
            segment_limit,
            failed: false,
        })
    }

    pub fn end(&self) -> u64 {
        self.checkpoint.end
    }
    pub fn segment_count(&self) -> usize {
        self.segments.len()
    }

    fn rotate(&mut self) -> io::Result<()> {
        let base = self.checkpoint.end;
        let temp_name = format!("{base:020}.tmp");
        let mut file = OpenOptions::new()
            .create_new(true)
            .read(true)
            .write(true)
            .open(self.dir.join(&temp_name))?;
        file.write_all(&segment_header(base))?;
        file.sync_all()?;
        fs::rename(self.dir.join(&temp_name), self.dir.join(name(base)))?;
        sync_dir(&self.dir)?;
        let checkpoint = Checkpoint {
            generation: self
                .checkpoint
                .generation
                .checked_add(1)
                .ok_or_else(|| invalid("generation overflow"))?,
            end: base,
            base,
            byte_end: SEGMENT_HEADER as u64,
        };
        save_checkpoint(&self.dir, checkpoint)?;
        self.log = file;
        self.checkpoint = checkpoint;
        self.segments.push(Segment {
            base,
            end: base,
            byte_end: SEGMENT_HEADER as u64,
            index: vec![],
        });
        Ok(())
    }

    pub fn append(&mut self, payload: &[u8]) -> io::Result<u64> {
        if self.failed {
            return Err(invalid("store failed; reopen after recovery"));
        }
        if self.checkpoint.end == u64::MAX {
            return Err(invalid("offset capacity reached"));
        }
        let record = encode_record(self.checkpoint.end, payload).map_err(invalid)?;
        if record.len() as u64 + SEGMENT_HEADER as u64 > self.segment_limit {
            return Err(invalid("record exceeds segment limit"));
        }
        let last = self.segments.last().unwrap();
        if last.byte_end + record.len() as u64 > self.segment_limit
            && self.segments.len() == MAX_SEGMENTS
        {
            return Err(invalid("segment capacity reached"));
        }
        let result = (|| {
            if self.segments.last().unwrap().byte_end + record.len() as u64 > self.segment_limit {
                self.rotate()?;
            }
            let last = self.segments.last_mut().unwrap();
            self.log.write_all_at(&record, last.byte_end)?;
            self.log.sync_all()?;
            let checkpoint = Checkpoint {
                generation: self
                    .checkpoint
                    .generation
                    .checked_add(1)
                    .ok_or_else(|| invalid("generation overflow"))?,
                end: self.checkpoint.end + 1,
                base: last.base,
                byte_end: last.byte_end + record.len() as u64,
            };
            save_checkpoint(&self.dir, checkpoint)?;
            if (checkpoint.end - 1 - last.base) % 64 == 0 {
                last.index.push((checkpoint.end - 1, last.byte_end));
            }
            last.end = checkpoint.end;
            last.byte_end = checkpoint.byte_end;
            self.checkpoint = checkpoint;
            Ok(checkpoint.end - 1)
        })();
        if result.is_err() {
            self.failed = true;
        }
        result
    }

    pub fn read(&self, offset: u64) -> io::Result<Option<Vec<u8>>> {
        let page = self.read_page(offset, 1, (MAX_PAYLOAD + RECORD_HEADER) as u32)?;
        Ok(page
            .records
            .into_iter()
            .next()
            .map(|bytes| bytes[RECORD_HEADER..].to_vec()))
    }

    pub fn read_page(&self, offset: u64, max_records: u32, max_bytes: u32) -> io::Result<Page> {
        if self.failed {
            return Err(invalid("store failed"));
        }
        if offset > self.checkpoint.end {
            return Err(invalid("offset out of range"));
        }
        if max_records == 0 || max_records > 1024 || max_bytes == 0 || max_bytes > 4 * 1024 * 1024 {
            return Err(invalid("invalid read limits"));
        }
        let mut page = Page {
            end: self.checkpoint.end,
            next: offset,
            records: vec![],
        };
        for segment in &self.segments {
            if page.next >= segment.end {
                continue;
            }
            let file = File::open(self.dir.join(name(segment.base)))?;
            let (mut current, mut position) = segment
                .index
                .iter()
                .rev()
                .find(|(o, _)| *o <= page.next)
                .copied()
                .unwrap_or((segment.base, SEGMENT_HEADER as u64));
            while current < segment.end && page.records.len() < max_records as usize {
                let (bytes, size) = record_at(&file, position, segment.byte_end, current)?
                    .ok_or_else(|| invalid("incomplete protected record"))?;
                if current >= page.next {
                    let used: usize = page.records.iter().map(Vec::len).sum();
                    if used + size > max_bytes as usize {
                        if page.records.is_empty() {
                            return Err(invalid(format!(
                                "buffer too small: requires {size} bytes"
                            )));
                        }
                        return Ok(page);
                    }
                    page.records.push(bytes);
                    page.next = current + 1;
                }
                position += size as u64;
                current += 1;
            }
            if page.records.len() >= max_records as usize {
                break;
            }
        }
        Ok(page)
    }
}

fn file_sync_last(dir: &Path, base: u64) -> io::Result<()> {
    File::open(dir.join(name(base)))?.sync_all()
}
