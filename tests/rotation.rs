use kubelog::storage::Store;
use std::{
    fs,
    os::unix::fs::FileExt,
    time::{SystemTime, UNIX_EPOCH},
};

#[test]
fn rotation_survives_restart_and_reads_across_segments() {
    let path = std::env::temp_dir().join(format!(
        "kubelog-rotation-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let mut store = Store::init_with_limit(&path, 100).unwrap();
    for i in 0..5 {
        assert_eq!(store.append(format!("message-{i}").as_bytes()).unwrap(), i);
    }
    assert_eq!(store.segment_count(), 3);
    drop(store);
    let store = Store::open_with_limit(&path, 100).unwrap();
    assert_eq!(store.end(), 5);
    assert_eq!(store.read(4).unwrap(), Some(b"message-4".to_vec()));
    let page = store.read_page(1, 5, 200).unwrap();
    assert_eq!(page.end, 5);
    assert_eq!(page.next, 5);
    assert_eq!(page.records.len(), 4);
    drop(store);
    fs::remove_dir_all(path).unwrap();
}

#[test]
fn protected_predecessor_cannot_be_damaged() {
    let path = std::env::temp_dir().join(format!(
        "kubelog-rotation-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let mut store = Store::init_with_limit(&path, 100).unwrap();
    for _ in 0..3 {
        store.append(b"123456789").unwrap();
    }
    drop(store);
    let file = fs::OpenOptions::new()
        .write(true)
        .open(path.join("00000000000000000000.log"))
        .unwrap();
    file.write_all_at(b"!", 56).unwrap();
    assert!(Store::open_with_limit(&path, 100).is_err());
    fs::remove_dir_all(path).unwrap();
}

#[test]
fn empty_successor_after_crash_finishes_rotation() {
    let path = std::env::temp_dir().join(format!(
        "kubelog-successor-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let mut store = Store::init_with_limit(&path, 100).unwrap();
    store.append(b"one").unwrap();
    drop(store);
    fs::write(
        path.join("00000000000000000001.log"),
        kubelog::format::segment_header(1),
    )
    .unwrap();
    let mut store = Store::open_with_limit(&path, 100).unwrap();
    assert_eq!(store.segment_count(), 2);
    assert_eq!(store.append(b"two").unwrap(), 1);
    drop(store);
    assert_eq!(Store::open_with_limit(&path, 100).unwrap().end(), 2);
    fs::remove_dir_all(path).unwrap();
}
