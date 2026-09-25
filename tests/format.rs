use kubelog::format::{Checkpoint, Decode, decode_record, encode_record, segment_header};

#[test]
fn record_bytes_and_crc_are_stable() {
    assert_eq!(crc32fast::hash(b"123456789"), 0xcbf4_3926);
    let bytes = encode_record(7, b"abc").unwrap();
    assert_eq!(&bytes[..4], b"KLGR");
    assert_eq!(
        &bytes[4..20],
        &[1, 0, 0, 0, 3, 0, 0, 0, 7, 0, 0, 0, 0, 0, 0, 0]
    );
    assert_eq!(bytes.len(), 27);
    assert_eq!(&bytes[20..24], &[0xcc, 0x00, 0xad, 0x89]);
    assert_eq!(
        decode_record(&bytes, 7).unwrap(),
        Decode::Complete {
            payload: b"abc".to_vec(),
            size: 27
        }
    );
    let mut corrupt = bytes;
    corrupt[26] ^= 1;
    assert!(decode_record(&corrupt, 7).is_err());
}

#[test]
fn truncation_is_distinct_from_corruption() {
    let bytes = encode_record(0, b"xyz").unwrap();
    for length in 0..bytes.len() {
        assert_eq!(
            decode_record(&bytes[..length], 0).unwrap(),
            Decode::Incomplete
        );
    }
    assert!(decode_record(&bytes, 1).is_err());
    assert!(encode_record(0, &vec![0; 1024 * 1024 + 1]).is_err());
}

#[test]
fn segment_and_checkpoint_roundtrip() {
    let segment = segment_header(8);
    assert_eq!(segment.len(), 32);
    assert_eq!(&segment[28..], &[0xe8, 0x6c, 0x2e, 0x35]);
    assert_eq!(kubelog::format::decode_segment(&segment).unwrap(), 8);
    let checkpoint = Checkpoint {
        generation: 2,
        end: 9,
        base: 8,
        byte_end: 55,
    };
    let encoded = checkpoint.encode();
    assert_eq!(encoded.len(), 48);
    assert_eq!(&encoded[44..], &[0x9b, 0x11, 0xc7, 0xcb]);
    assert_eq!(Checkpoint::decode(&encoded).unwrap(), checkpoint);
    let mut damaged = encoded;
    damaged[20] ^= 1;
    assert!(Checkpoint::decode(&damaged).is_err());
}
