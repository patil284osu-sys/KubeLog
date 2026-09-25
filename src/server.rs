use crate::{engine::Engine, format::MAX_PAYLOAD};
use std::{
    io::{self, Read, Write},
    net::{TcpListener, TcpStream},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    thread,
    time::{Duration, Instant},
};

const TIMEOUT: Duration = Duration::from_secs(10);
const MAX_CONNECTIONS: usize = 32;

fn receive(stream: &mut TcpStream, output: &mut [u8], deadline: Instant) -> io::Result<bool> {
    let mut filled = 0;
    while filled < output.len() {
        let left = deadline.saturating_duration_since(Instant::now());
        if left.is_zero() {
            return Err(io::ErrorKind::TimedOut.into());
        }
        stream.set_read_timeout(Some(left))?;
        match stream.read(&mut output[filled..]) {
            Ok(0) if filled == 0 => return Ok(false),
            Ok(0) => return Err(io::ErrorKind::UnexpectedEof.into()),
            Ok(n) => filled += n,
            Err(err) if err.kind() == io::ErrorKind::Interrupted => continue,
            Err(err) => return Err(err),
        }
    }
    Ok(true)
}

fn send(stream: &mut TcpStream, op: u8, id: u64, body: &[u8]) -> io::Result<()> {
    let mut frame = Vec::with_capacity(20 + body.len());
    frame.extend(b"KLGN");
    frame.extend(1u16.to_be_bytes());
    frame.push(op);
    frame.push(0);
    frame.extend(id.to_be_bytes());
    frame.extend((body.len() as u32).to_be_bytes());
    frame.extend(body);
    let deadline = Instant::now() + TIMEOUT;
    let mut written = 0;
    while written < frame.len() {
        let left = deadline.saturating_duration_since(Instant::now());
        if left.is_zero() {
            return Err(io::ErrorKind::TimedOut.into());
        }
        stream.set_write_timeout(Some(left))?;
        match stream.write(&frame[written..]) {
            Ok(0) => return Err(io::ErrorKind::WriteZero.into()),
            Ok(n) => written += n,
            Err(err) if err.kind() == io::ErrorKind::Interrupted => continue,
            Err(err) => return Err(err),
        }
    }
    Ok(())
}

fn error(
    stream: &mut TcpStream,
    id: u64,
    code: u16,
    outcome: u8,
    required: u32,
    message: &str,
) -> io::Result<()> {
    let mut end = message.len().min(256);
    while !message.is_char_boundary(end) {
        end -= 1;
    }
    let message = &message.as_bytes()[..end];
    let mut body = Vec::with_capacity(10 + message.len());
    body.extend(code.to_be_bytes());
    body.push(outcome);
    body.push(0);
    body.extend(required.to_be_bytes());
    body.extend((message.len() as u16).to_be_bytes());
    body.extend(message);
    send(stream, 255, id, &body)
}

fn client(mut stream: TcpStream, engine: Arc<Engine>) -> io::Result<()> {
    loop {
        let deadline = Instant::now() + TIMEOUT;
        let mut header = [0; 20];
        if !receive(&mut stream, &mut header, deadline)? {
            return Ok(());
        }
        if &header[..4] != b"KLGN" {
            return Ok(());
        }
        let id = u64::from_be_bytes(header[8..16].try_into().unwrap());
        let version = u16::from_be_bytes(header[4..6].try_into().unwrap());
        let op = header[6];
        let length = u32::from_be_bytes(header[16..20].try_into().unwrap()) as usize;
        if length > 4 * 1024 * 1024 + 20
            || (op == 1 && length > MAX_PAYLOAD + 1)
            || (op == 2 && length > 16)
            || (op == 3 && length > 0)
        {
            return Ok(());
        }
        let mut body = vec![0; length];
        if !receive(&mut stream, &mut body, deadline)? {
            return Ok(());
        }
        let outcome = if op == 1 { 0 } else { 2 };
        if version != 1 {
            error(
                &mut stream,
                id,
                2,
                outcome,
                0,
                "unsupported protocol version",
            )?;
            continue;
        }
        if header[7] != 0 {
            error(&mut stream, id, 1, outcome, 0, "unsupported frame flags")?;
            continue;
        }
        match op {
            1 => {
                if body.is_empty() {
                    error(&mut stream, id, 1, 0, 0, "missing ack mode")?;
                    continue;
                }
                if body[0] != 1 {
                    error(&mut stream, id, 3, 0, 0, "unsupported ack mode")?;
                    continue;
                }
                match engine.append(body[1..].to_vec()) {
                    Ok(offset) => {
                        let mut response = Vec::with_capacity(16);
                        response.extend(offset.to_be_bytes());
                        response.extend((offset + 1).to_be_bytes());
                        send(&mut stream, 129, id, &response)?;
                    }
                    Err(message) => {
                        let (code, outcome) = if message.contains("overloaded") {
                            (4, 0)
                        } else if message.contains("capacity") || message.contains("segment limit")
                        {
                            (10, 0)
                        } else if message == "engine unavailable" {
                            (5, 0)
                        } else if message == "payload too large" {
                            (1, 0)
                        } else {
                            (6, 1)
                        };
                        error(&mut stream, id, code, outcome, 0, &message)?;
                    }
                }
            }
            2 => {
                if body.len() != 16 {
                    error(&mut stream, id, 1, 2, 0, "read needs 16 bytes")?;
                    continue;
                }
                let offset = u64::from_be_bytes(body[..8].try_into().unwrap());
                let records = u32::from_be_bytes(body[8..12].try_into().unwrap());
                let bytes = u32::from_be_bytes(body[12..16].try_into().unwrap());
                if records == 0 || records > 1024 || bytes == 0 || bytes > 4 * 1024 * 1024 {
                    error(&mut stream, id, 1, 2, 0, "invalid read limits")?;
                    continue;
                }
                match engine.read(offset, records, bytes) {
                    Ok(page) => {
                        let mut response = Vec::with_capacity(20 + bytes as usize);
                        response.extend(page.end.to_be_bytes());
                        response.extend(page.next.to_be_bytes());
                        response.extend((page.records.len() as u32).to_be_bytes());
                        for record in page.records {
                            response.extend(record);
                        }
                        send(&mut stream, 130, id, &response)?;
                    }
                    Err(message) => {
                        let required = message
                            .strip_prefix("buffer too small: requires ")
                            .and_then(|n| n.strip_suffix(" bytes"))
                            .and_then(|n| n.parse().ok())
                            .unwrap_or(0);
                        let code = if required != 0 {
                            8
                        } else if message.contains("out of range") {
                            7
                        } else {
                            6
                        };
                        error(&mut stream, id, code, 2, required, &message)?;
                    }
                }
            }
            3 => {
                let state = engine.describe();
                let mut response = vec![if state.healthy { 1 } else { 3 }];
                response.extend([0; 7]);
                response.extend(state.end.to_be_bytes());
                response.extend(state.end.to_be_bytes());
                response.extend((state.segments as u32).to_be_bytes());
                response.extend((state.queued as u32).to_be_bytes());
                response.extend((state.bytes as u64).to_be_bytes());
                send(&mut stream, 131, id, &response)?;
            }
            _ => error(&mut stream, id, 1, 2, 0, "unknown operation")?,
        }
    }
}

pub fn serve(listener: TcpListener, engine: Arc<Engine>, stop: Arc<AtomicBool>) -> io::Result<()> {
    listener.set_nonblocking(true)?;
    let active = Arc::new(AtomicUsize::new(0));
    while !stop.load(Ordering::Relaxed) {
        match listener.accept() {
            Ok((stream, _)) => {
                if active.fetch_add(1, Ordering::SeqCst) >= MAX_CONNECTIONS {
                    active.fetch_sub(1, Ordering::SeqCst);
                    drop(stream);
                    continue;
                }
                let active = Arc::clone(&active);
                let engine = Arc::clone(&engine);
                thread::spawn(move || {
                    let _ = client(stream, engine);
                    active.fetch_sub(1, Ordering::SeqCst);
                });
            }
            Err(err) if err.kind() == io::ErrorKind::WouldBlock => {
                thread::sleep(Duration::from_millis(10))
            }
            Err(err) => return Err(err),
        }
    }
    engine.stop_admission();
    let deadline = Instant::now() + TIMEOUT;
    while active.load(Ordering::SeqCst) != 0 && Instant::now() < deadline {
        thread::sleep(Duration::from_millis(10));
    }
    Ok(())
}
