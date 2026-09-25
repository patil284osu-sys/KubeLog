use kubelog::{engine::Engine, server::serve, storage::Store};
use std::{
    fs,
    io::{Read, Write},
    net::{TcpListener, TcpStream},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    thread,
    time::{SystemTime, UNIX_EPOCH},
};

fn exchange(stream: &mut TcpStream, op: u8, id: u64, body: &[u8]) -> (u8, Vec<u8>) {
    let mut frame = vec![];
    frame.extend(b"KLGN");
    frame.extend(1u16.to_be_bytes());
    frame.push(op);
    frame.push(0);
    frame.extend(id.to_be_bytes());
    frame.extend((body.len() as u32).to_be_bytes());
    frame.extend(body);
    for chunk in frame.chunks(3) {
        stream.write_all(chunk).unwrap();
    }
    let mut header = [0; 20];
    stream.read_exact(&mut header).unwrap();
    assert_eq!(&header[..4], b"KLGN");
    assert_eq!(u64::from_be_bytes(header[8..16].try_into().unwrap()), id);
    let mut answer = vec![0; u32::from_be_bytes(header[16..20].try_into().unwrap()) as usize];
    stream.read_exact(&mut answer).unwrap();
    (header[6], answer)
}

#[test]
fn fragmented_append_read_describe_and_restart() {
    let path = std::env::temp_dir().join(format!(
        "kubelog-tcp-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let engine = Arc::new(Engine::start(Store::init(&path).unwrap()));
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let stop = Arc::new(AtomicBool::new(false));
    let running = thread::spawn({
        let engine = Arc::clone(&engine);
        let stop = Arc::clone(&stop);
        move || serve(listener, engine, stop).unwrap()
    });
    let mut client = TcpStream::connect(addr).unwrap();
    let (op, rejected) = exchange(&mut client, 1, 11, &[2, b'x']);
    assert_eq!(op, 255);
    assert_eq!(u16::from_be_bytes(rejected[..2].try_into().unwrap()), 3);
    assert_eq!(rejected[2], 0);
    let (op, response) = exchange(&mut client, 1, 12, &[1, b'h', b'i']);
    assert_eq!(op, 129);
    assert_eq!(response.len(), 16);
    assert_eq!(u64::from_be_bytes(response[..8].try_into().unwrap()), 0);
    let (op, response) = exchange(
        &mut client,
        2,
        13,
        &[0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 100],
    );
    assert_eq!(op, 130);
    assert_eq!(u32::from_be_bytes(response[16..20].try_into().unwrap()), 1);
    assert_eq!(&response[44..], b"hi");
    let (op, too_small) = exchange(
        &mut client,
        2,
        15,
        &[0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 1],
    );
    assert_eq!(op, 255);
    assert_eq!(u16::from_be_bytes(too_small[..2].try_into().unwrap()), 8);
    assert_eq!(u32::from_be_bytes(too_small[4..8].try_into().unwrap()), 26);
    let (op, response) = exchange(&mut client, 3, 14, &[]);
    assert_eq!(op, 131);
    assert_eq!(u64::from_be_bytes(response[16..24].try_into().unwrap()), 1);
    drop(client);
    stop.store(true, Ordering::Relaxed);
    running.join().unwrap();
    engine.shutdown();
    drop(engine);
    assert_eq!(
        Store::open(&path).unwrap().read(0).unwrap(),
        Some(b"hi".to_vec())
    );
    fs::remove_dir_all(path).unwrap();
}
