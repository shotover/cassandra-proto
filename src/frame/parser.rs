use super::*;
use crate::compression::Compressor;
use crate::frame::frame_error::{AdditionalErrorInfo, CDRSError, SimpleError};
use crate::frame::FromCursor;
use crate::types::data_serialization_types::decode_timeuuid;
use crate::types::CString;
use crate::types::{from_bytes, from_u16_bytes, CStringList, UUID_LEN};
use bytes::BytesMut;
use std::io::{Cursor, Read};
use uuid;

#[derive(Debug, Clone, Copy)]
pub struct FrameHeader {
    version: Version,
    flags: u8, //Vec<Flag>,
    stream: u16,
    opcode: Opcode,
    length: usize,
}

fn make_protocol_error(msg: &str) -> CDRSError {
    CDRSError {
        error_code: 0x000A, // protocol error
        message: CString::new(msg.to_string()),
        additional_info: AdditionalErrorInfo::Protocol(SimpleError {}),
    }
}

fn make_server_error(msg: &str) -> CDRSError {
    CDRSError {
        error_code: 0x0000, // server error
        message: CString::new(msg.to_string()),
        additional_info: AdditionalErrorInfo::Protocol(SimpleError {}),
    }
}

/// Reads the header from the input stream.
/// If there is not enough data in the buffer will return None.
///
fn read_header(src: &mut BytesMut) -> Result<Option<FrameHeader>, CDRSError> {
    let head_len = 9;
    let max_frame_len = 1024 * 1024 * 15; //15MB

    // Make sure we have some data
    if src.len() == 0 {
        return Ok(None);
    }
    // we have at least one byte so read the version.  We read the version byte first so that
    // we can handle versions that have a different header size properly by reporting them as
    // invalid or unsupported.
    let version: Version = match src.first() {
        Some(x) => {
            let v = Version::from(*x);
            match v {
                Version::Other(_c) => {
                    // do not change error string, it is part of the defined protocol
                    return Err(make_protocol_error(
                        "Invalid or unsupported protocol version",
                    ))
                }
                _ => v,
            }
        }
        // no first byte -- should not happen as we already checked buffer length
        None => return Err(make_server_error("Can not read protocol version")),
    };

    // make sure we have enough data to read the header.
    if src.len() < head_len {
        return Ok(None);
    }

    let mut version_bytes = [0; Version::BYTE_LENGTH];
    let mut flag_bytes = [0; Flag::BYTE_LENGTH];
    let mut opcode_bytes = [0; Opcode::BYTE_LENGTH];
    let mut stream_bytes = [0; STREAM_LEN];
    let mut length_bytes = [0; LENGTH_LEN];
    let head_buf = src.split_to(head_len);
    let mut cursor = Cursor::new(head_buf);

    // NOTE: order of reads matters
    // We handle the error at the end of this block.
    let mut y = cursor.read_exact(&mut version_bytes);
    if y.is_ok() {
        y = cursor.read_exact(&mut flag_bytes);
    }
    if y.is_ok() {
        y = cursor.read_exact(&mut stream_bytes);
    }
    if y.is_ok() {
        y = cursor.read_exact(&mut opcode_bytes);
    }
    if y.is_ok() {
        y = cursor.read_exact(&mut length_bytes);
    }
    if y.is_err() {
        return Err(make_server_error(&y.unwrap_err().to_string()));
    }

    // check for error length problem.
    let header_length = from_bytes(&length_bytes) as usize;
    if header_length > max_frame_len {
        return Err(make_protocol_error("Max frame length exceeded"));
    }

    Ok(Some(FrameHeader {
        version,
        flags: flag_bytes[0],
        stream: from_u16_bytes(&stream_bytes),
        opcode: Opcode::from(opcode_bytes[0]),
        length: header_length,
    }))
}

/// Parses the input bytes into a Cassandra frame.
/// //
/// This function may be called repeatedly.  The initial call should pass None as the argument for
/// the `frame_header_original` argument.  On subsequent calls the FrameHeader returned from the
/// previous call should be passed.
///
/// # Returns
/// * (None,None) if there is not enough data to read the header
/// * (None,FrameHeader) if there is enough data for the header but not enough for the body.
/// * (Frame,None) when the entire frame has been read.
/// * CDRSError
///   * Protocol Error (0x000A) when the version does not match or the max frame length is exceeded
///   * Server Error (0x0000) if the buffers can not be read or other internal errors.
///
/// # Arguments
/// * `src` - The input byte stream to read.
/// * `compressor` - The compression implementation for compressed data.
/// * `frame_header_original - The frame header extracted in an earlier attempt to construct this
/// frame.
pub fn parse_frame<E>(
    mut src: &mut BytesMut,
    compressor: &dyn Compressor<CompressorError = E>,
    frame_header_original: Option<FrameHeader>,
) -> Result<(Option<Frame>, Option<FrameHeader>), CDRSError>
where
    E: std::error::Error,
{
    let head_len = 9;

    // If the frame_header_original is set then we are trying to parse the data
    // and have already extracted the header.

    let frame_header: FrameHeader = match frame_header_original {
        Some(x) => x,
        None => {
            let r = read_header(src);
            if r.is_err() {
                return r.map(|_y| (None, None));
            } else {
                match r.ok() {
                    Some(x) => x.unwrap(),
                    None => return Ok((None, None)),
                }
            }
        }
    };

    // check that there buffer contains all the bytes for the body.
    if src.len() < frame_header.length {
        // make sure there is space for the body
        src.reserve(frame_header.length - src.len());
        return Ok((None, Some(frame_header)));
    }
    let full_body = read_body(&frame_header, compressor, &mut src)?;

    let mut body_cursor = Cursor::new(full_body.as_slice());

    let frame = Frame {
        version: frame_header.version,
        flags: Flag::get_collection(frame_header.flags),
        stream: frame_header.stream,
        opcode: frame_header.opcode,
        tracing_id: extract_tracing_id(&frame_header, &mut body_cursor)?,
        warnings: extract_warnings(&frame_header, &mut body_cursor)?,
        body: extract_body(&mut body_cursor)?,
    };

    src.reserve(head_len);
    Ok((Some(frame), None))
}

/// Reads the body from a Cursor.  
///
/// Length of body is determined by `frame_header.length`.
///
/// # Arguments
/// * `frame_header` - The header for the frame being parsed.
/// * `compressor` - The compressor implementation to decompress the body with.
/// * `cursor` - The cursor to read the body from.
///
/// # Returns
/// * The body of the frame.
/// * CDRSError - Server Error (0x0000) if the entire body can not be read or if the compressor
/// fails.
///
fn read_body<E>(
    frame_header: &FrameHeader,
    compressor: &dyn Compressor<CompressorError = E>,
    src: &mut BytesMut,
) -> Result<Vec<u8>, CDRSError>
where
    E: std::error::Error,
{
    if src.len() < frame_header.length {
        return Err(make_server_error( "Not enough data for body"));
    }
    let mut cursor = Cursor::new(src.split_to(frame_header.length));
    let mut body_bytes = Vec::with_capacity(frame_header.length);
    unsafe {
        body_bytes.set_len(frame_header.length);
    }

    let y = cursor
        .read_exact(&mut body_bytes)
        .map_err(|x| make_server_error(&x.to_string()));
    if y.is_err() {
        Err(y.unwrap_err())
    } else {
        Ok(if Flag::has_compression(frame_header.flags) {
            compressor
                .decode(body_bytes)
                .map_err(|x| make_server_error(&*format!("{} while reading body", x.to_string())))?
        } else {
            body_bytes
        })
    }
}

/// Extracts the tracing ID from the current cursor position if the `frame_header.flags` contains
/// the Tracing flag.
///
/// If the flag is not set, returns `None` otherwise returns the UUID that is the tracing ID.
/// # Arguments
/// * `frame_header` - The header for the frame being parsed.
/// * `cursor` - The cursor to read the tracing id from.
///
fn extract_tracing_id(
    frame_header: &FrameHeader,
    cursor: &mut Cursor<&[u8]>,
) -> Result<Option<uuid::Uuid>, CDRSError> {
    // Use cursor to get tracing id, warnings and actual body

    if Flag::has_tracing(frame_header.flags) {
        let mut tracing_bytes = [0 as u8; UUID_LEN];

        let x = cursor
            .read_exact(&mut tracing_bytes)
            .map_err(|x| make_server_error(&*format!("{} while reading tracing id", x.to_string())));

        if x.is_err() {
            Err(x.unwrap_err())
        } else {
            decode_timeuuid(&tracing_bytes)
                .map(|x| Some(x))
                .map_err(|x| make_server_error(&x.to_string()))
        }
    } else {
        Ok(None)
    }
}

/// Extracts the warnings from the current cursor position if the `frame_header.flags` contains
/// the Warning flag.
///
/// If the flag is not set, returns an empty Vec otherwise returns the Vec of warning messages.
/// # Arguments
/// * `frame_header` - The header for the frame being parsed.
/// * `cursor` - The cursor to read the warnings from.
///
fn extract_warnings(
    frame_header: &FrameHeader,
    cursor: &mut Cursor<&[u8]>,
) -> Result<Vec<String>, CDRSError> {
    if Flag::has_warning(frame_header.flags) {
        CStringList::from_cursor(cursor)
            .map_err(|x| make_server_error(&*format!("{} while extracting warnings", x.to_string())))
            .map(|x| x.into_plain())
    } else {
        Ok(vec![])
    }
}

/// Extracts the body from a cursor.
fn extract_body(cursor: &mut Cursor<&[u8]>) -> Result<Vec<u8>, CDRSError> {
    let mut body = vec![];
    let x = cursor
        .read_to_end(&mut body)
        .map_err(|x| make_server_error(&*format!("{} while extracting body", x.to_string())));
    if x.is_err() {
        Err(x.unwrap_err())
    } else {
        Ok(body)
    }
}

//fn convert_frame_into_result(frame: Frame) -> error::Result<(Option<Frame>, Option<FrameHeader>)> {
//    match frame.opcode {
//        Opcode::Error => frame.get_body().and_then(|err| match err {
//            ResponseBody::Error(err) => Err(error::Error::Server(err)),
//            _ => unreachable!(),
//        }),
//        _ => Ok((Some(frame), None)),
//    }
//}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::frame::traits::AsByte;
    use bytes::{BytesMut, BufMut};
    use crate::compressors::no_compression::NoCompression;


    #[test]
    #[cfg(not(feature = "v3"))]
    fn test_frame_version_as_byte() {
        let request_version = Version::Request;
        assert_eq!(request_version.as_byte(), 0x04);
        let response_version = Version::Response;
        assert_eq!(response_version.as_byte(), 0x84);
    }

    #[test]
    fn test_read_header_invalid_version() {
        let mut buf = BytesMut::with_capacity(64);

        buf.put_u8(0x42);

        let r = read_header(&mut buf);
        assert_eq!( r.is_err(), true );
        let err = r.unwrap_err() ;
        assert_eq!( err.error_code, 0x000A ); // protocol error
        assert_eq!( err.message.as_str(), "Invalid or unsupported protocol version" );
    }

    #[test]
    fn test_read_header_no_header() {
        let mut buf = BytesMut::with_capacity(64);

        let r = read_header(&mut buf);
        assert_eq!( r.is_ok(), true );
        let opt = r.unwrap() ;
        assert_eq!(opt.is_none(), true ); // no header read
    }

    #[test]
    fn test_read_header() {
        let mut buf = BytesMut::with_capacity(64);

        /*
      +---------+---------+---------+---------+---------+
      | version |  flags  |      stream       | opcode  |
      +---------+---------+---------+---------+---------+
      |                length                 |
      +---------+---------+---------+---------+
         */
        buf.put_u8(0x4); // version
        buf.put_u8( 0x2 ); // flags
        buf.put_u16( 0x0102 ); //stream
        buf.put_u8( 0x05 ); // opcode (Option)
        buf.put_u32( 500 ); // length

        let r = read_header(&mut buf);
        assert_eq!( r.is_ok(), true );
        let opt = r.unwrap() ;
        assert_eq!(opt.is_some(), true ); // no header read
        let header = opt.unwrap();
        assert_eq!( header.version, Version::Request );
        assert_eq!( header.flags, 0x2 );
        assert_eq!( header.stream, 0x0102 );
        assert_eq!( header.opcode, Opcode::Options );
        assert_eq!( header.length, 500 );
    }


    #[test]
    fn test_read_body() {
        let mut buf = BytesMut::with_capacity(64);
        let compression = NoCompression::new();

        // put in the header
        /*
      +---------+---------+---------+---------+---------+
      | version |  flags  |      stream       | opcode  |
      +---------+---------+---------+---------+---------+
      |                length                 |
      +---------+---------+---------+---------+
         */
        buf.put_u8(0x4); // version
        buf.put_u8( 0x2 ); // flags (Tracing)
        buf.put_u16( 0x0102 ); //stream
        buf.put_u8( 0x05 ); // opcode (Option)
        buf.put_u32( 11 ); // length

        // put in the body
        buf.put( "Hello World".as_bytes());
        let r = read_header(&mut buf);
        assert_eq!( r.is_ok(), true );
        let opt = r.unwrap() ;
        assert_eq!(opt.is_some(), true ); // no header read
        let header = opt.unwrap();
        assert_eq!( header.version, Version::Request );
        assert_eq!( header.flags, 0x2 );
        assert_eq!( header.stream, 0x0102 );
        assert_eq!( header.opcode, Opcode::Options );
        assert_eq!( header.length, 11 );
        let b = read_body( &header, &compression, &mut buf );
        assert_eq!( b.is_ok(), true );
        let body = b.unwrap();
        assert_eq!( body.as_slice(), "Hello World".as_bytes());
    }

    #[test]
    fn test_read_short_body() {
        let mut buf = BytesMut::with_capacity(64);
        let compression = NoCompression::new();

        // put in the header
        /*
      +---------+---------+---------+---------+---------+
      | version |  flags  |      stream       | opcode  |
      +---------+---------+---------+---------+---------+
      |                length                 |
      +---------+---------+---------+---------+
         */
        buf.put_u8(0x4); // version
        buf.put_u8( 0x2 ); // flags (Tracing)
        buf.put_u16( 0x0102 ); //stream
        buf.put_u8( 0x05 ); // opcode (Option)
        buf.put_u32( 15 ); // length

        // put in the body
        buf.put( "Hello World".as_bytes());
        let r = read_header(&mut buf);
        let header = r.unwrap().unwrap();
        assert_eq!( header.length, 15 );
        let b = read_body( &header, &compression, &mut buf );
        assert_eq!( b.is_err(), true );
        let err = b.unwrap_err();
        assert_eq!( err.error_code, 0x0000 ); // server error
        assert_eq!( err.message.as_str(), "Not enough data for body" );
    }

    #[test]
    fn test_read_long_body() {
        let mut buf = BytesMut::with_capacity(64);
        let compression = NoCompression::new();

        // put in the header
        /*
      +---------+---------+---------+---------+---------+
      | version |  flags  |      stream       | opcode  |
      +---------+---------+---------+---------+---------+
      |                length                 |
      +---------+---------+---------+---------+
         */
        buf.put_u8(0x4); // version
        buf.put_u8( 0x2 ); // flags (Tracing)
        buf.put_u16( 0x0102 ); //stream
        buf.put_u8( 0x05 ); // opcode (Option)
        buf.put_u32( 11 ); // length

        // put in the body
        buf.put( "Hello WorldNow is the time".as_bytes());
        let r = read_header(&mut buf);
        let header = r.unwrap().unwrap();
        assert_eq!( header.length, 11 );
        let b = read_body( &header, &compression, &mut buf );
        assert_eq!( b.is_ok(), true );
        let body = b.unwrap();
        assert_eq!( body.as_slice(), "Hello World".as_bytes());
        assert_eq!( buf.len(), 15);
    }

    #[test]
    fn test_extract_tracing_id() {
        let mut buf = BytesMut::with_capacity(64);
        let compression = NoCompression::new();

        // put in the header
        /*
      +---------+---------+---------+---------+---------+
      | version |  flags  |      stream       | opcode  |
      +---------+---------+---------+---------+---------+
      |                length                 |
      +---------+---------+---------+---------+
         */
        buf.put_u8(0x4); // version
        buf.put_u8( 0x2 ); // flags (Tracing)
        buf.put_u16( 0x0102 ); //stream
        buf.put_u8( 0x05 ); // opcode (Option)
        buf.put_u32( 19 ); // length

        let uuid  = Uuid::new_v4();
        for i in uuid.as_bytes() {
            buf.put_u8(*i);
        }
        // put in the body
        buf.put( "Hello World".as_bytes());
        let r = read_header(&mut buf);
        assert_eq!( r.is_ok(), true );
        let opt = r.unwrap() ;
        assert_eq!(opt.is_some(), true ); // no header read
        let header = opt.unwrap();
        assert_eq!( header.flags, 0x2 );
        assert_eq!( header.length, 19 );
        let b = read_body( &header, &compression, &mut buf );
        let v = b.unwrap();
        let mut cursor = Cursor::new(v.as_slice());
        let r = extract_tracing_id( &header, &mut cursor);
        assert_eq!( r.is_ok(), true );
        assert_eq!( r.unwrap().unwrap(), uuid);
    }

    #[test]
    fn test_extract_tracing_id_no_flag() {
        let mut buf = BytesMut::with_capacity(64);
        let compression = NoCompression::new();

        // put in the header
        /*
      +---------+---------+---------+---------+---------+
      | version |  flags  |      stream       | opcode  |
      +---------+---------+---------+---------+---------+
      |                length                 |
      +---------+---------+---------+---------+
         */
        buf.put_u8(0x4); // version
        buf.put_u8( 0x0 ); // flags
        buf.put_u16( 0x0102 ); //stream
        buf.put_u8( 0x05 ); // opcode (Option)
        buf.put_u32( 11 ); // length

        // put in the body
        buf.put( "Hello World".as_bytes());
        let r = read_header(&mut buf);
        assert_eq!( r.is_ok(), true );
        let opt = r.unwrap() ;
        assert_eq!(opt.is_some(), true ); // no header read
        let header = opt.unwrap();
        assert_eq!( header.flags, 0x0 );
        assert_eq!( header.length, 11 );
        let b = read_body( &header, &compression, &mut buf );
        let v = b.unwrap();
        let mut cursor = Cursor::new(v.as_slice());
        let r = extract_tracing_id( &header, &mut cursor);
        assert_eq!( r.is_ok(), true );
        assert_eq!( r.unwrap().is_none(), true);
    }

    #[test]
    fn test_extract_tracing_id_no_data() {
        let mut buf = BytesMut::with_capacity(64);
        let compression = NoCompression::new();

        // put in the header
        /*
      +---------+---------+---------+---------+---------+
      | version |  flags  |      stream       | opcode  |
      +---------+---------+---------+---------+---------+
      |                length                 |
      +---------+---------+---------+---------+
         */
        buf.put_u8(0x4); // version
        buf.put_u8( 0x2 ); // flags (Tracing)
        buf.put_u16( 0x0102 ); //stream
        buf.put_u8( 0x05 ); // opcode (Option)
        buf.put_u32( 0 ); // length

        let r = read_header(&mut buf);
        assert_eq!( r.is_ok(), true );
        let opt = r.unwrap() ;
        assert_eq!(opt.is_some(), true ); // no header read
        let header = opt.unwrap();
        assert_eq!( header.flags, 0x2 );
        assert_eq!( header.length, 0 );
        let b = read_body( &header, &compression, &mut buf );
        let v = b.unwrap();
        let mut cursor = Cursor::new(v.as_slice());
        let r = extract_tracing_id( &header, &mut cursor);
        assert_eq!( r.is_err(), true );
        let err = r.unwrap_err();
        assert_eq!( err.error_code, 0x0000 ); // server error
        assert_eq!( err.message.as_str(), "failed to fill whole buffer while reading tracing id" );
    }

    #[test]
    fn test_extract_warnings() {
        let mut buf = BytesMut::with_capacity(64);
        let compression = NoCompression::new();

        // put in the header
        /*
      +---------+---------+---------+---------+---------+
      | version |  flags  |      stream       | opcode  |
      +---------+---------+---------+---------+---------+
      |                length                 |
      +---------+---------+---------+---------+
         */
        buf.put_u8(0x4); // version
        buf.put_u8( 0x8 ); // flags (Warning)
        buf.put_u16( 0x0102 ); //stream
        buf.put_u8( 0x05 ); // opcode (Option)
        buf.put_u32( 2+11+2+16+13 ); // length (see segments below)

        // string list comprising 2 strings
        buf.put_u16( 2 );
        // put in hello world
        buf.put_u16( 11);
        buf.put( "Hello World".as_bytes());
        // put in "now is the time.."
        buf.put_u16( 16 );
        buf.put( "Now is the time.".as_bytes());

        // put in the body (13 bytes)
        buf.put( "What is this?".as_bytes());

        let r = read_header(&mut buf);
        assert_eq!( r.is_ok(), true );
        let opt = r.unwrap() ;
        assert_eq!(opt.is_some(), true ); // no header read
        let header = opt.unwrap();
        assert_eq!( header.flags, 0x8 );
        assert_eq!( header.length, 2+11+2+16+13 );
        let b = read_body( &header, &compression, &mut buf );
        let v = b.unwrap();
        let mut cursor = Cursor::new(v.as_slice());
        let r = extract_warnings( &header, &mut cursor);
        assert_eq!( r.is_ok(), true );
        let warnings = r.unwrap();
        assert_eq!( warnings[0], "Hello World");
        assert_eq!( warnings[1], "Now is the time.");
    }

    #[test]
    fn test_extract_warnings_no_flag() {
        let mut buf = BytesMut::with_capacity(64);
        let compression = NoCompression::new();

        // put in the header
        /*
      +---------+---------+---------+---------+---------+
      | version |  flags  |      stream       | opcode  |
      +---------+---------+---------+---------+---------+
      |                length                 |
      +---------+---------+---------+---------+
         */
        buf.put_u8(0x4); // version
        buf.put_u8( 0x0 ); // flags
        buf.put_u16( 0x0102 ); //stream
        buf.put_u8( 0x05 ); // opcode (Option)
        buf.put_u32( 2+11+2+16+13 ); // length (see segments below)

        // string list comprising 2 strings
        buf.put_u16( 2 );
        // put in hello world
        buf.put_u16( 11);
        buf.put( "Hello World".as_bytes());
        // put in "now is the time.."
        buf.put_u16( 16 );
        buf.put( "Now is the time.".as_bytes());

        // put in the body (13 bytes)
        buf.put( "What is this?".as_bytes());

        let r = read_header(&mut buf);
        assert_eq!( r.is_ok(), true );
        let opt = r.unwrap() ;
        assert_eq!(opt.is_some(), true ); // no header read
        let header = opt.unwrap();
        assert_eq!( header.flags, 0x0 );
        assert_eq!( header.length, 2+11+2+16+13 );
        let b = read_body( &header, &compression, &mut buf );
        let v = b.unwrap();
        let mut cursor = Cursor::new(v.as_slice());
        let r = extract_warnings( &header, &mut cursor);
        assert_eq!( r.is_ok(), true );
        let warnings = r.unwrap();
        assert_eq!( warnings.len(), 0);
    }

    #[test]
    fn test_extract_warnings_no_data() {
        let mut buf = BytesMut::with_capacity(64);
        let compression = NoCompression::new();

        // put in the header
        /*
      +---------+---------+---------+---------+---------+
      | version |  flags  |      stream       | opcode  |
      +---------+---------+---------+---------+---------+
      |                length                 |
      +---------+---------+---------+---------+
         */
        buf.put_u8(0x4); // version
        buf.put_u8( 0x08 ); // flags (Warnings)
        buf.put_u16( 0x0102 ); //stream
        buf.put_u8( 0x05 ); // opcode (Option)
        buf.put_u32( 0 ); // length


        let r = read_header(&mut buf);
        assert_eq!( r.is_ok(), true );
        let opt = r.unwrap() ;
        assert_eq!(opt.is_some(), true ); // no header read
        let header = opt.unwrap();
        assert_eq!( header.flags, 0x8 );
        assert_eq!( header.length, 0 );
        let b = read_body( &header, &compression, &mut buf );
        let v = b.unwrap();
        let mut cursor = Cursor::new(v.as_slice());
        let r = extract_warnings( &header, &mut cursor);
        assert_eq!( r.is_err(), true );
        let err = r.unwrap_err();
        assert_eq!( err.error_code, 0x0000 ); // server error
        assert_eq!( err.message.as_str(), "IO error: failed to fill whole buffer while extracting warnings" );
    }

    /*
    pub fn parse_frame<E>(
    mut src: &mut BytesMut,
    compressor: &dyn Compressor<CompressorError = E>,
    frame_header_original: Option<FrameHeader>,
) -> Result<(Option<Frame>, Option<FrameHeader>), CDRSError>
     */
    #[test]
    fn test_parse_frame() {
        let mut buf = BytesMut::with_capacity(64);
        let compression = NoCompression::new();

        // put in the header
        /*
      +---------+---------+---------+---------+---------+
      | version |  flags  |      stream       | opcode  |
      +---------+---------+---------+---------+---------+
      |                length                 |
      +---------+---------+---------+---------+
         */
        buf.put_u8(0x4); // version
        buf.put_u8( 0x2 | 0x8 ); // flags (Tracing & Warnings)
        buf.put_u16( 0x0102 ); //stream
        buf.put_u8( 0x05 ); // opcode (Option)
        let len = 16+2+2+11+2+16+13;
        buf.put_u32( len ); // length (see segments below)

        let uuid  = Uuid::new_v4(); // 16 bytes
        for i in uuid.as_bytes() {
            buf.put_u8(*i);
        }

        // string list comprising 2 strings
        buf.put_u16( 2 );   // 2 bytes
        // put in hello world
        buf.put_u16( 11);   // 2 bytes
        buf.put( "Hello World".as_bytes()); // 11 bytes
        // put in "now is the time.."
        buf.put_u16( 16 );   // 2 bytes
        buf.put( "Now is the time.".as_bytes()); // 16 bytes

        // put in the body (13 bytes)
        buf.put( "What is this?".as_bytes());  // 13 bytes


        let r = parse_frame( &mut buf, &compression, None  );
        assert_eq!( r.is_ok(), true );
        let (frame_opt, header_opt) = r.unwrap() ;
        assert_eq!(frame_opt.is_some(), true );
        assert_eq!(header_opt.is_none(), true );
        let frame = frame_opt.unwrap();
        assert_eq!( frame.flags.contains( &Flag::Tracing), true );
        assert_eq!( frame.flags.contains( &Flag::Warning), true );
        assert_eq!( frame.stream, 0x0102);
        assert_eq!( frame.opcode, Opcode::Options);
        assert_eq!( frame.tracing_id.is_some(), true);
        assert_eq!( frame.tracing_id.unwrap(), uuid );
        assert_eq!( frame.warnings.len(), 2 );
        assert_eq!( frame.warnings[0], "Hello World" );
        assert_eq!( frame.warnings[1], "Now is the time." );
        assert_eq!( frame.body, "What is this?".as_bytes());
    }

}
