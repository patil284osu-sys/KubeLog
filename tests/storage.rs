use kubelog::storage::Store;
use std::{
    fs,
    os::unix::fs::FileExt,
    path::PathBuf,
    time::{SystemTime, UNIX_EPOCH},
};

fn dir() -> PathBuf {
    let path = std::env::temp_dir().join(format!(
        "kubelog-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    path
}

#[test]
fn acknowledged_records_survive_reopen_and_lock_is_exclusive() {
    let path = dir();
    let mut store = Store::init(&path).unwrap();
    use std::os::unix::fs::PermissionsExt;
    assert_eq!(
        fs::metadata(&path).unwrap().permissions().mode() & 0o777,
        0o700
    );
    assert!(Store::open(&path).is_err());
    assert_eq!(store.append(b"first").unwrap(), 0);
    assert_eq!(store.append(b"").unwrap(), 1);
    assert_eq!(store.read(0).unwrap(), Some(b"first".to_vec()));
    drop(store);
    let store = Store::open(&path).unwrap();
    assert_eq!(store.end(), 2);
    assert_eq!(store.read(1).unwrap(), Some(vec![]));
    assert_eq!(store.read(2).unwrap(), None);
    drop(store);
    fs::remove_dir_all(path).unwrap();
}

#[test]
fn unacknowledged_complete_suffix_is_preserved_and_incomplete_tail_is_removed() {
    let path = dir();
    let mut store = Store::init(&path).unwrap();
    store.append(b"safe").unwrap();
    drop(store);
    let file = fs::OpenOptions::new()
        .append(true)
        .open(path.join("00000000000000000000.log"))
        .unwrap();
    let mut tail = kubelog::format::encode_record(1, b"complete").unwrap();
    tail.extend(&kubelog::format::encode_record(2, b"partial").unwrap()[..25]);
    file.write_all_at(&tail, file.metadata().unwrap().len())
        .unwrap();
    drop(file);
    let store = Store::open(&path).unwrap();
    assert_eq!(store.end(), 2);
    assert_eq!(store.read(1).unwrap(), Some(b"complete".to_vec()));
    drop(store);
    assert_eq!(
        fs::metadata(path.join("00000000000000000000.log"))
            .unwrap()
            .len(),
        32 + 28 + 32
    );
    fs::remove_dir_all(path).unwrap();
}

#[test]
fn damaged_protected_record_fails_closed() {
    let path = dir();
    let mut store = Store::init(&path).unwrap();
    store.append(b"safe").unwrap();
    drop(store);
    let file = fs::OpenOptions::new()
        .write(true)
        .open(path.join("00000000000000000000.log"))
        .unwrap();
    file.write_all_at(b"X", 56).unwrap();
    drop(file);
    assert!(Store::open(&path).is_err());
    fs::remove_dir_all(path).unwrap();
}

#[test]
fn full_invalid_suffix_and_missing_checkpoint_fail_closed() {
    let path = dir();
    let mut store = Store::init(&path).unwrap();
    store.append(b"protected").unwrap();
    drop(store);
    let file = fs::OpenOptions::new()
        .append(true)
        .open(path.join("00000000000000000000.log"))
        .unwrap();
    let mut invalid = kubelog::format::encode_record(1, b"suffix").unwrap();
    *invalid.last_mut().unwrap() ^= 1;
    file.write_all_at(&invalid, file.metadata().unwrap().len())
        .unwrap();
    drop(file);
    assert!(Store::open(&path).is_err());
    fs::remove_file(path.join("checkpoint")).unwrap();
    assert!(Store::open(&path).is_err());
    fs::remove_dir_all(path).unwrap();
}
