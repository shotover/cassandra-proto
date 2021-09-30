use crate::compression::Compressor;
use crate::error::Error;

#[derive(Debug)]
pub struct NoCompression {}

impl NoCompression {
    pub fn new() -> NoCompression {
        return NoCompression {};
    }
}

impl Compressor for NoCompression {
    type CompressorError = Error;

    fn encode(&self, bytes: Vec<u8>) -> Result<Vec<u8>, Self::CompressorError> {
        Ok(bytes)
    }

    fn decode(&self, bytes: Vec<u8>) -> Result<Vec<u8>, Self::CompressorError> {
        Ok(bytes)
    }

    fn into_string(&self) -> Option<String> {
        None
    }
}
