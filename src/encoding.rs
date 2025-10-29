use bytes::{BufMut, BytesMut};
use std::io;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum EncodingError {
    #[error("IO error: {0}")]
    Io(#[from] io::Error),
    #[error("Invalid UTF-8: {0}")]
    Utf8(#[from] std::str::Utf8Error),
    #[error("String too long: {length} > {max}")]
    StringTooLong { length: usize, max: usize },
    #[error("Invalid length: expected {expected}, got {actual}")]
    InvalidLength { expected: usize, actual: usize },
    #[error("Invalid enum value: {0}")]
    InvalidEnumValue(u8),
}

pub trait Encode {
    fn encode(&self, buf: &mut BytesMut) -> Result<(), EncodingError>;

    fn encoded_size(&self) -> usize;
}

pub trait Decode: Sized {
    fn decode(buf: &[u8]) -> Result<(Self, usize), EncodingError>;
}

impl Encode for u8 {
    fn encode(&self, buf: &mut BytesMut) -> Result<(), EncodingError> {
        buf.put_u8(*self);
        Ok(())
    }

    fn encoded_size(&self) -> usize {
        1
    }
}

impl Encode for u16 {
    fn encode(&self, buf: &mut BytesMut) -> Result<(), EncodingError> {
        buf.put_u16_le(*self);
        Ok(())
    }

    fn encoded_size(&self) -> usize {
        2
    }
}

impl Encode for u32 {
    fn encode(&self, buf: &mut BytesMut) -> Result<(), EncodingError> {
        buf.put_u32_le(*self);
        Ok(())
    }

    fn encoded_size(&self) -> usize {
        4
    }
}

impl Encode for u64 {
    fn encode(&self, buf: &mut BytesMut) -> Result<(), EncodingError> {
        buf.put_u64_le(*self);
        Ok(())
    }

    fn encoded_size(&self) -> usize {
        8
    }
}

impl Encode for bool {
    fn encode(&self, buf: &mut BytesMut) -> Result<(), EncodingError> {
        buf.put_u8(if *self { 1 } else { 0 });
        Ok(())
    }

    fn encoded_size(&self) -> usize {
        1
    }
}

impl<T: Encode> Encode for Option<T> {
    fn encode(&self, buf: &mut BytesMut) -> Result<(), EncodingError> {
        match self {
            None => buf.put_u8(0),
            Some(val) => {
                buf.put_u8(1);
                val.encode(buf)?;
            }
        }
        Ok(())
    }

    fn encoded_size(&self) -> usize {
        1 + self.as_ref().map(|v| v.encoded_size()).unwrap_or(0)
    }
}

impl<T: Encode, const N: usize> Encode for [T; N] {
    fn encode(&self, buf: &mut BytesMut) -> Result<(), EncodingError> {
        for item in self {
            item.encode(buf)?;
        }
        Ok(())
    }

    fn encoded_size(&self) -> usize {
        self.iter().map(|item| item.encoded_size()).sum()
    }
}

impl Encode for [u8] {
    fn encode(&self, buf: &mut BytesMut) -> Result<(), EncodingError> {
        buf.extend_from_slice(self);
        Ok(())
    }

    fn encoded_size(&self) -> usize {
        self.len()
    }
}

impl<T: Encode> Encode for Vec<T> {
    fn encode(&self, buf: &mut BytesMut) -> Result<(), EncodingError> {
        encode_variable_length(self.len() as u64, buf)?;
        for item in self {
            item.encode(buf)?;
        }
        Ok(())
    }

    fn encoded_size(&self) -> usize {
        let len_size = variable_length_size(self.len() as u64);
        let items_size: usize = self.iter().map(|item| item.encoded_size()).sum();
        len_size + items_size
    }
}

pub fn encode_variable_length(mut value: u64, buf: &mut BytesMut) -> Result<(), EncodingError> {
    loop {
        let byte = (value & 0x7F) as u8;
        value >>= 7;
        if value == 0 {
            buf.put_u8(byte);
            break;
        } else {
            buf.put_u8(byte | 0x80);
        }
    }
    Ok(())
}

pub fn variable_length_size(mut value: u64) -> usize {
    let mut size = 0;
    loop {
        size += 1;
        value >>= 7;
        if value == 0 {
            break;
        }
    }
    size
}

pub fn encode_message<T: Encode>(msg: &T) -> Result<Vec<u8>, EncodingError> {
    let msg_size = msg.encoded_size();
    let mut buf = BytesMut::with_capacity(4 + msg_size);

    buf.put_u32_le(msg_size as u32);

    msg.encode(&mut buf)?;

    Ok(buf.to_vec())
}
