use kubelog::{engine::Engine, storage::Store};
use std::{
    fs,
    sync::Arc,
    thread,
    time::{SystemTime, UNIX_EPOCH},
};

#[test]
fn concurrent_producers_get_contiguous_durable_offsets() {
    let path = std::env::temp_dir().join(format!(
        "kubelog-engine-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let engine = Arc::new(Engine::start(Store::init(&path).unwrap()));
    let handles: Vec<_> = (0..20)
        .map(|i| {
            let engine = Arc::clone(&engine);
            thread::spawn(move || {
                (
                    i,
                    engine.append(format!("record-{i}").into_bytes()).unwrap(),
                )
            })
        })
        .collect();
    let mut results: Vec<_> = handles.into_iter().map(|h| h.join().unwrap()).collect();
    results.sort_by_key(|(_, offset)| *offset);
    assert_eq!(
        results.iter().map(|(_, o)| *o).collect::<Vec<_>>(),
        (0..20).collect::<Vec<_>>()
    );
    for (i, offset) in &results {
        assert_eq!(
            engine.read(*offset, 1, 100).unwrap().records[0][24..],
            format!("record-{i}").as_bytes()[..]
        );
    }
    engine.shutdown();
    drop(engine);
    let store = Store::open(&path).unwrap();
    assert_eq!(store.end(), 20);
    drop(store);
    fs::remove_dir_all(path).unwrap();
}

#[test]
fn capacity_rejection_keeps_previous_records_readable() {
    let path = std::env::temp_dir().join(format!(
        "kubelog-capacity-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let engine = Engine::start(Store::init_with_limit(&path, 60).unwrap());
    assert!(
        engine
            .append(vec![0; 5])
            .unwrap_err()
            .contains("segment limit")
    );
    assert_eq!(engine.append(vec![]).unwrap(), 0);
    assert_eq!(engine.read(0, 1, 24).unwrap().records.len(), 1);
    engine.shutdown();
    drop(engine);
    fs::remove_dir_all(path).unwrap();
}

#[test]
fn damaged_read_marks_engine_unhealthy() {
    use std::os::unix::fs::FileExt;
    let path = std::env::temp_dir().join(format!(
        "kubelog-read-damage-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let engine = Engine::start(Store::init(&path).unwrap());
    engine.append(b"record".to_vec()).unwrap();
    let file = fs::OpenOptions::new()
        .write(true)
        .open(path.join("00000000000000000000.log"))
        .unwrap();
    file.write_all_at(b"!", 56).unwrap();
    assert!(engine.read(0, 1, 100).is_err());
    assert!(!engine.describe().healthy);
    engine.shutdown();
    drop(engine);
    fs::remove_dir_all(path).unwrap();
}
