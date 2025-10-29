use bytes::BytesMut;
use std::io::Cursor;
use tart_backend::decoder::{decode_message_frame, decode_variable_length, Decode};
use tart_backend::encoding::*;

#[test]
fn test_u8_encoding_decoding() {
    let test_cases = vec![0u8, 1, 127, 128, 255];

    for value in test_cases {
        let mut buf = BytesMut::new();
        value.encode(&mut buf).unwrap();
        assert_eq!(buf.len(), 1);
        assert_eq!(value.encoded_size(), 1);

        let mut cursor = Cursor::new(&buf[..]);
        let decoded = u8::decode(&mut cursor).unwrap();
        assert_eq!(decoded, value);
    }
}

#[test]
fn test_u16_encoding_decoding() {
    let test_cases = vec![0u16, 1, 255, 256, 32767, 32768, 65535];

    for value in test_cases {
        let mut buf = BytesMut::new();
        value.encode(&mut buf).unwrap();
        assert_eq!(buf.len(), 2);
        assert_eq!(value.encoded_size(), 2);

        // Check little-endian encoding
        assert_eq!(buf[0], (value & 0xFF) as u8);
        assert_eq!(buf[1], ((value >> 8) & 0xFF) as u8);

        let mut cursor = Cursor::new(&buf[..]);
        let decoded = u16::decode(&mut cursor).unwrap();
        assert_eq!(decoded, value);
    }
}

#[test]
fn test_u32_encoding_decoding() {
    let test_cases = vec![0u32, 1, 65535, 65536, 2147483647, 2147483648, 4294967295];

    for value in test_cases {
        let mut buf = BytesMut::new();
        value.encode(&mut buf).unwrap();
        assert_eq!(buf.len(), 4);
        assert_eq!(value.encoded_size(), 4);

        // Check little-endian encoding
        for i in 0..4 {
            assert_eq!(buf[i], ((value >> (i * 8)) & 0xFF) as u8);
        }

        let mut cursor = Cursor::new(&buf[..]);
        let decoded = u32::decode(&mut cursor).unwrap();
        assert_eq!(decoded, value);
    }
}

#[test]
fn test_u64_encoding_decoding() {
    let test_cases = vec![
        0u64,
        1,
        4294967295,
        4294967296,
        9223372036854775807,
        9223372036854775808,
        18446744073709551615,
    ];

    for value in test_cases {
        let mut buf = BytesMut::new();
        value.encode(&mut buf).unwrap();
        assert_eq!(buf.len(), 8);
        assert_eq!(value.encoded_size(), 8);

        // Check little-endian encoding
        for i in 0..8 {
            assert_eq!(buf[i], ((value >> (i * 8)) & 0xFF) as u8);
        }

        let mut cursor = Cursor::new(&buf[..]);
        let decoded = u64::decode(&mut cursor).unwrap();
        assert_eq!(decoded, value);
    }
}

#[test]
fn test_bool_encoding_decoding() {
    // Test false
    let mut buf = BytesMut::new();
    false.encode(&mut buf).unwrap();
    assert_eq!(buf.len(), 1);
    assert_eq!(buf[0], 0);
    assert_eq!(false.encoded_size(), 1);

    let mut cursor = Cursor::new(&buf[..]);
    let decoded = bool::decode(&mut cursor).unwrap();
    assert!(!decoded);

    // Test true
    buf.clear();
    true.encode(&mut buf).unwrap();
    assert_eq!(buf.len(), 1);
    assert_eq!(buf[0], 1);

    let mut cursor = Cursor::new(&buf[..]);
    let decoded = bool::decode(&mut cursor).unwrap();
    assert!(decoded);
}

#[test]
fn test_bool_decode_invalid() {
    let invalid_values = vec![2u8, 3, 10, 255];

    for value in invalid_values {
        let buf = [value];
        let mut cursor = Cursor::new(&buf[..]);
        assert!(bool::decode(&mut cursor).is_err());
    }
}

#[test]
fn test_option_encoding_decoding() {
    // Test None
    let none_value: Option<u32> = None;
    let mut buf = BytesMut::new();
    none_value.encode(&mut buf).unwrap();
    assert_eq!(buf.len(), 1);
    assert_eq!(buf[0], 0);
    assert_eq!(none_value.encoded_size(), 1);

    let mut cursor = Cursor::new(&buf[..]);
    let decoded = Option::<u32>::decode(&mut cursor).unwrap();
    assert_eq!(decoded, None);

    // Test Some with various values
    let test_cases = vec![Some(0u32), Some(42), Some(u32::MAX)];

    for value in test_cases {
        buf.clear();
        value.encode(&mut buf).unwrap();
        assert_eq!(buf[0], 1); // Some discriminator
        assert_eq!(buf.len(), 5); // 1 byte discriminator + 4 bytes u32
        assert_eq!(value.encoded_size(), 5);

        let mut cursor = Cursor::new(&buf[..]);
        let decoded = Option::<u32>::decode(&mut cursor).unwrap();
        assert_eq!(decoded, value);
    }
}

#[test]
fn test_fixed_array_encoding_decoding() {
    // Test [u8; 32] (common for hashes)
    let hash: [u8; 32] = [42; 32];
    let mut buf = BytesMut::new();
    hash.encode(&mut buf).unwrap();
    assert_eq!(buf.len(), 32);
    assert_eq!(hash.encoded_size(), 32);

    let mut cursor = Cursor::new(&buf[..]);
    let decoded = <[u8; 32]>::decode(&mut cursor).unwrap();
    assert_eq!(decoded, hash);

    // Test [u8; 16] (IPv6 address)
    let ipv6: [u8; 16] = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16];
    buf.clear();
    ipv6.encode(&mut buf).unwrap();
    assert_eq!(buf.len(), 16);

    let mut cursor = Cursor::new(&buf[..]);
    let decoded = <[u8; 16]>::decode(&mut cursor).unwrap();
    assert_eq!(decoded, ipv6);
}

#[test]
fn test_variable_length_encoding() {
    let test_cases = vec![
        (0u64, vec![0]),
        (1, vec![1]),
        (127, vec![127]),
        (128, vec![128, 1]),
        (255, vec![255, 1]),
        (256, vec![128, 2]),
        (16383, vec![255, 127]),
        (16384, vec![128, 128, 1]),
        (99999, vec![159, 141, 6]), // Large but below 100k limit
    ];

    for (value, expected) in test_cases {
        let mut buf = BytesMut::new();
        encode_variable_length(value, &mut buf).unwrap();
        assert_eq!(buf.to_vec(), expected);
        assert_eq!(variable_length_size(value), expected.len());

        let mut cursor = Cursor::new(&buf[..]);
        let decoded = decode_variable_length(&mut cursor).unwrap();
        assert_eq!(decoded, value);
    }
}

#[test]
fn test_vec_u8_encoding_decoding() {
    let test_cases = vec![
        vec![],
        vec![0u8],
        vec![1, 2, 3, 4, 5],
        vec![255; 128], // Length boundary
        vec![42; 256],  // Larger than single byte length
    ];

    for value in test_cases {
        let mut buf = BytesMut::new();
        value.encode(&mut buf).unwrap();

        // Check encoded size
        let expected_size = variable_length_size(value.len() as u64) + value.len();
        assert_eq!(buf.len(), expected_size);
        assert_eq!(value.encoded_size(), expected_size);

        let mut cursor = Cursor::new(&buf[..]);
        let decoded = Vec::<u8>::decode(&mut cursor).unwrap();
        assert_eq!(decoded, value);
    }
}

#[test]
fn test_bounded_string_encoding_decoding() {
    use tart_backend::types::BoundedString;

    // Test empty string
    let empty = BoundedString::<32>::new("").unwrap();
    let mut buf = BytesMut::new();
    empty.encode(&mut buf).unwrap();
    assert_eq!(buf.len(), 1); // Just length byte (0)

    let mut cursor = Cursor::new(&buf[..]);
    let decoded = BoundedString::<32>::decode(&mut cursor).unwrap();
    assert_eq!(decoded.as_str().unwrap(), "");

    // Test ASCII string
    let ascii = BoundedString::<32>::new("Hello, JAM!").unwrap();
    buf.clear();
    ascii.encode(&mut buf).unwrap();
    assert_eq!(buf[0], 11); // Length
    assert_eq!(&buf[1..], b"Hello, JAM!");

    let mut cursor = Cursor::new(&buf[..]);
    let decoded = BoundedString::<32>::decode(&mut cursor).unwrap();
    assert_eq!(decoded.as_str().unwrap(), "Hello, JAM!");

    // Test UTF-8 string
    let utf8 = BoundedString::<32>::new("Hello 🚀").unwrap();
    buf.clear();
    utf8.encode(&mut buf).unwrap();

    let mut cursor = Cursor::new(&buf[..]);
    let decoded = BoundedString::<32>::decode(&mut cursor).unwrap();
    assert_eq!(decoded.as_str().unwrap(), "Hello 🚀");

    // Test maximum length string (32 bytes)
    let max_str = "a".repeat(32);
    let max_bounded = BoundedString::<32>::new(&max_str).unwrap();
    buf.clear();
    max_bounded.encode(&mut buf).unwrap();

    let mut cursor = Cursor::new(&buf[..]);
    let decoded = BoundedString::<32>::decode(&mut cursor).unwrap();
    assert_eq!(decoded.as_str().unwrap(), max_str);
}

#[test]
fn test_bounded_string_too_long() {
    use tart_backend::types::BoundedString;

    let too_long = "a".repeat(33);
    assert!(BoundedString::<32>::new(&too_long).is_err());

    // Test decoding string that's too long
    let mut buf = BytesMut::new();
    encode_variable_length(33, &mut buf).unwrap();
    buf.extend_from_slice(&[b'a'; 33]);

    let mut cursor = Cursor::new(&buf[..]);
    // After our resilience improvements, oversized strings are truncated instead of failing
    let result = BoundedString::<32>::decode(&mut cursor);
    assert!(
        result.is_ok(),
        "Decoder should recover from oversized strings by truncating"
    );
}

#[test]
fn test_bounded_string_invalid_utf8() {
    use tart_backend::types::BoundedString;

    // Try to decode invalid UTF-8
    let mut buf = BytesMut::new();
    encode_variable_length(4, &mut buf).unwrap();
    buf.extend_from_slice(&[0xFF, 0xFE, 0xFD, 0xFC]); // Invalid UTF-8

    let mut cursor = Cursor::new(&buf[..]);
    // After our resilience improvements, invalid UTF-8 is recovered using lossy conversion
    let result = BoundedString::<32>::decode(&mut cursor);
    assert!(
        result.is_ok(),
        "Decoder should recover from invalid UTF-8 using lossy conversion"
    );
}

#[test]
fn test_message_frame_encoding() {
    let test_data: Vec<u8> = vec![1, 2, 3, 4, 5];
    let encoded = encode_message(&test_data).unwrap();

    // Check message frame structure
    assert_eq!(encoded.len(), 4 + test_data.encoded_size());

    // Check size prefix (little-endian u32)
    let size_bytes = &encoded[0..4];
    let size = u32::from_le_bytes([size_bytes[0], size_bytes[1], size_bytes[2], size_bytes[3]]);
    assert_eq!(size as usize, test_data.encoded_size());

    // Decode the frame
    let (decoded_size, content) = decode_message_frame(&encoded).unwrap();
    assert_eq!(decoded_size as usize, test_data.encoded_size());
    assert_eq!(content.len(), test_data.encoded_size());
}

#[test]
fn test_decode_insufficient_data() {
    // Test u32 with insufficient data
    let buf = [1, 2, 3]; // Only 3 bytes, need 4
    let mut cursor = Cursor::new(&buf[..]);
    assert!(u32::decode(&mut cursor).is_err());

    // Test array with insufficient data
    let buf = [0u8; 31]; // Only 31 bytes, need 32
    let mut cursor = Cursor::new(&buf[..]);
    assert!(<[u8; 32]>::decode(&mut cursor).is_err());
}

#[test]
fn test_message_frame_size_limits() {
    // Test empty message
    let empty: Vec<u8> = vec![];
    let encoded = encode_message(&empty).unwrap();
    assert_eq!(encoded.len(), 4 + 1); // 4 bytes size + 1 byte length (0)

    // Test decoding with message too large check
    let mut buf = vec![0xFF, 0xFF, 0xFF, 0xFF]; // Max u32 size
    buf.extend_from_slice(&[0; 100]); // Some data

    // This should fail due to size limit check (>10MB)
    assert!(decode_message_frame(&buf).is_err());
}
