use std::io::{Cursor, Read};
use super::*;
use crate::compression::Compressor;
use crate::error;
use std::io;
use crate::frame::frame_response::ResponseBody;
use crate::frame::FromCursor;
use crate::types::data_serialization_types::decode_timeuuid;
use crate::types::{from_bytes, from_u16_bytes, CStringList, UUID_LEN};
use bytes::{BytesMut};

#[derive(Debug, Clone)]
pub struct FrameHeader {
    version:  Version,
    flags: Vec<Flag>,
    stream: u16,
    opcode : Opcode,
    length : usize,
}

pub fn parse_frame<E>(
    src: & mut BytesMut,
    compressor: &dyn Compressor<CompressorError = E>,
    frame_header_original: &Option<FrameHeader>
) -> error::Result<(Option<Frame>, Option<FrameHeader>)>
where
    E: std::error::Error,
{
    // Make sure we have enough data
    let head_len = 9;
    let max_frame_len = 1024 * 1024 * 15; //15MB
    let frame_header: FrameHeader;

    if src.len() < head_len {
        // Not enough data to read the head
        return Ok((None, None));
    }

    let version:  Version;
    let flags: Vec<Flag>;
    let stream: u16;
    let opcode : Opcode;
    let length : usize;

    // if we have a previous frame header, use that instead
    //
    if let Some(header) = frame_header_original {
        version = header.version;
        flags = header.flags.clone();
        stream = header.stream;
        opcode = header.opcode;
        length = header.length;

    } else {
        let mut version_bytes = [0; Version::BYTE_LENGTH];
        let mut flag_bytes = [0; Flag::BYTE_LENGTH];
        let mut opcode_bytes = [0; Opcode::BYTE_LENGTH];
        let mut stream_bytes = [0; STREAM_LEN];
        let mut length_bytes = [0; LENGTH_LEN];
        let head_buf = src.split_to(head_len);
        let mut cursor = Cursor::new(head_buf);

        // NOTE: order of reads matters
        cursor.read_exact(&mut version_bytes)?;
        cursor.read_exact(&mut flag_bytes)?;
        cursor.read_exact(&mut stream_bytes)?;
        cursor.read_exact(&mut opcode_bytes)?;
        cursor.read_exact(&mut length_bytes)?;

        flags = Flag::get_collection(flag_bytes[0]);
        stream = from_u16_bytes(&stream_bytes);
        opcode = Opcode::from(opcode_bytes[0]);
        length = from_bytes(&length_bytes) as usize;
        version = if opcode.as_byte( ) == Opcode::Options.as_byte() {
            Version::request
        } else {
            Version::from(version_bytes.to_vec())
        }
    }

    frame_header = FrameHeader {
        version,
        flags,
        stream,
        opcode,
        length
    };

    if (frame_header.length as usize) > max_frame_len {
        //TODO throw error
        return Err(error::Error::Io(
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "Max frame length exceeded",
            )
        ));
    }
    src.reserve(frame_header.length);

    if src.len() < frame_header.length {
        return Ok((None, Some(frame_header)));
    }

    let mut body_bytes = Vec::with_capacity(frame_header.length);
    unsafe {
        body_bytes.set_len(frame_header.length);
    }

    let mut outer_body_cursor = Cursor::new(src.split_to(frame_header.length));
    outer_body_cursor.read_exact(&mut body_bytes)?;

    let full_body = if frame_header.flags.iter().any(|flag| flag == &Flag::Compression) {
        compressor
            .decode(body_bytes)
            .map_err(|err| error::Error::from(err.description()))?
    } else {
        body_bytes
    };

    // Use cursor to get tracing id, warnings and actual body
    let mut body_cursor = Cursor::new(full_body.as_slice());

    let tracing_id = if frame_header.flags.iter().any(|flag| flag == &Flag::Tracing) {
        let mut tracing_bytes = Vec::with_capacity(UUID_LEN);
        unsafe {
            tracing_bytes.set_len(UUID_LEN);
        }
        body_cursor.read_exact(&mut tracing_bytes)?;

        decode_timeuuid(tracing_bytes.as_slice()).ok()
    } else {
        None
    };

    let warnings = if frame_header.flags.iter().any(|flag| flag == &Flag::Warning) {
        CStringList::from_cursor(&mut body_cursor)?.into_plain()
    } else {
        vec![]
    };

    let mut body = vec![];

    body_cursor.read_to_end(&mut body)?;

    let frame = Frame {
        version: frame_header.version,
        flags: frame_header.flags,
        opcode: frame_header.opcode,
        stream: frame_header.stream,
        body: body,
        tracing_id: tracing_id,
        warnings: warnings,
    };

    src.reserve(head_len);
    // convert_frame_into_result(frame)
    Ok((Some(frame), None))
}

fn convert_frame_into_result(frame: Frame) -> error::Result<(Option<Frame>, Option<FrameHeader>)> {
    match frame.opcode {
        Opcode::Error => frame.get_body().and_then(|err| match err {
            ResponseBody::Error(err) => Err(error::Error::Server(err)),
            _ => unreachable!(),
        }),
        _ => Ok((Some(frame), None)),
    }
}
