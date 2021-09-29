use std::io::{Cursor, Read};
use super::*;
use crate::compression::Compressor;
use crate::error;
use std::io;
use crate::types::CString;
use crate::frame::frame_response::ResponseBody;
use crate::frame::frame_error::{CDRSError,AdditionalErrorInfo,SimpleError};
use crate::frame::FromCursor;
use crate::types::data_serialization_types::decode_timeuuid;
use crate::types::{from_bytes, from_u16_bytes, CStringList, UUID_LEN};
use bytes::{BytesMut, BufMut};


#[derive(Debug, Clone)]
pub struct FrameHeader {
    version:  Version,
    flags: Vec<Flag>,
    stream: u16,
    opcode : Opcode,
    length : usize,
}

fn make_protocol_error( &str: msg ) -> Err<CDRSError> {
    Err(CDRSError
    { error_code: 0x000A, // protocol error
        message: CString::new(msg ),
        additional_info: AdditionalErrorInfo::Protocol(SimpleError{}),
    })
}

fn read_header( src : & mut BytesMut ) -> Optional<FrameHeader> {
    let max_frame_len = 1024 * 1024 * 15; //15MB
    let mut version_bytes = [src.first()?; Version::BYTE_LENGTH];

    let version = Version::from(version_bytes.to_vec());
    if version == Version::Other {
        return make_protocol_error("Invalid or unsupported protocol version");
    }

    if src.len() < head_len {
        // Not enough data to read the head
        return Ok(None);
    }

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

    let header_length = from_bytes(&length_bytes) as usize;
    if (header_length > max_frame_len {
        return make_protocol_error( "Max frame length exceeded" );
    }

     OK(FrameHeader {
        version,
        flags: Flag::get_collection(flag_bytes[0]),
        stream: from_u16_bytes(&stream_bytes),
        opcode: Opcode::from(opcode_bytes[0]),
        length: header_length,
    })

}
pub fn parse_frame<E>(
    src: & mut BytesMut,
    compressor: &dyn Compressor<CompressorError = E>,
    frame_header_original: &Option<FrameHeader>
) -> Result<(Option<Frame>,Option<FrameHeader>), CDRSError>
where
    E: std::error::Error,
{
    // Make sure we have enough data
    if src.len() == 0 {
        // No data to read
        return Ok((None,None));
    }

    let head_len = 9;

    // If the frame_header_original is set then we are trying to parse the data
    // and have already extracted the header.

    let mut frame_header :Optional<FrameHeader> = if frame_header_original.is_some() {
        frame_header_origional
    } else { read_header( src )};
    if frame_header.is_none() {
        return Ok(None,None);
    }

    // make sure there is space for the body
    src.reserve(frame_header.length);

    if src.len() < frame_header.length {
        return Ok((None, Some(frame_header)));
    }
    let full_body = read_body( &frame_header,  Cursor::new(src.split_to(frame_header.length)));

    let mut body_cursor = Cursor::new(full_body.as_slice());

    let frame = Frame {
        version : frame_header.version,
        flags : frame_header.flags,
        stream : frame_header.stream,
        opcode : frame_header.opcode,
        tracing_id: extract_tracing_id( &frame_header, &mut body_cursor ),
        warnings: extract_warnings( &frame_header, &mut body_cursor ),
        body: extract_body(&mut body_cursor ),
    };

    src.reserve(head_len);
     Ok((Some(frame), None))
}

/// Reads the body from a Cursor.  length of body is determined by `frame_header.length`.
fn read_body( frame_header : & FrameHeader,  cursor : &mut Cursor<BytesMulti> ) -> Vec<u8> {

    let mut body_bytes = Vec::with_capacity(frame_header.length);
    unsafe {
        body_bytes.set_len(frame_header.length);
    }

    cursor.read_exact(&mut body_bytes)?;

    if frame_header.flags.iter().any(|flag| flag == &Flag::Compression) {
        compressor
            .decode(body_bytes)
            .map_err(|err| error::Error::from(err.description()))?
    } else {
        body_bytes
    };
}
/// Extracts the tracing ID from the current cursor position if the `frame_header.flags` contains
/// the Tracing flag.
fn extract_tracing_id(  frame_header :  & FrameHeader,  cursor : &mut Cursor<&[u8]>  ) -> Option<uuid> {
    // Use cursor to get tracing id, warnings and actual body

    let tracing_id = if frame_header.flags.iter().any(|flag| flag == &Flag::Tracing) {
        let mut tracing_bytes = [ 8; UUID_LEN ];

        cursor.read_exact(& tracing_bytes)?;

        decode_timeuuid(&tracing_bytes).ok()
    } else {
        None
    };
}

fn extract_warnings( frame_header :  & FrameHeader, cursor :  &mut Cursor<&[u8]> ) -> Vec<String> {
    let warnings = if frame_header.flags.iter().any(|flag| flag == &Flag::Warning) {
        CStringList::from_cursor(&mut cursor)?.into_plain()
    } else {
        vec![]
    };
}

fn extract_body( cursor : &mut Cursor<&[u8]> ) -> Vec<u8> {
    let mut body = vec![];
    cursor.read_to_end(&mut body)?;
    return body;
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
