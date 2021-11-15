use std::error;
use std::fmt;
use std::fmt::Display;
use std::io;
use std::result;
use std::string::FromUtf8Error;
use uuid::Error as UUIDError;

use crate::frame::frame_error::CDRSError;

pub type Result<T> = result::Result<T, Error>;

/// CDRS custom error type. CDRS expects two types of error - errors returned by Server
/// and internal erros occured within the driver itself. Ocassionaly `io::Error`
/// is a type that represent internal error because due to implementation IO errors only
/// can be raised by CDRS driver. `Server` error is an error which are ones returned by
/// a Server via result error frames.
#[derive(Debug)]
pub enum Error {
    /// Internal IO error.
    Io(io::Error),
    /// Internal error that may be raised during `uuid::Uuid::from_bytes`
    UUIDParse(UUIDError),
    /// General error
    General(String),
    /// Internal error that may be raised during `String::from_utf8`
    FromUtf8(FromUtf8Error),
    /// Internal Compression/Decompression error
    Compression(String),
    /// Server error.
    Server(CDRSError),
}

pub fn column_is_empty_err<T: Display>(column_name: T) -> Error {
    Error::General(format!("Column or UDT property '{}' is empty", column_name))
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            Error::Io(ref err) => write!(f, "IO error: {}", err),
            Error::Compression(ref err) => write!(f, "Compressor error: {}", err),
            Error::Server(ref err) => write!(f, "Server error: {:?}", err.message),
            Error::FromUtf8(ref err) => write!(f, "FromUtf8Error error: {:?}", err),
            Error::UUIDParse(ref err) => write!(f, "UUIDParse error: {:?}", err),
            Error::General(ref err) => write!(f, "GeneralParsing error: {:?}", err),
        }
    }
}

impl error::Error for Error {
    fn description(&self) -> &str {
        match *self {
            // TODO: Rewrite to avoid use of deprecated description method.
            // Doing this would require entirely rewriting the error API
            // due to the suggested replacements for description returning a String instead of &str
            // So we should come back later and replace cassandra-protocol error handling with thiserror or similar.
            #[allow(deprecated)]
            Error::Io(ref err) => err.description(),
            Error::Compression(ref err) => err.as_str(),
            Error::Server(ref err) => err.message.as_str(),
            #[allow(deprecated)]
            Error::FromUtf8(ref err) => err.description(),
            // FIXME: err.description not found in current scope, std::error::Error not satisfied
            Error::UUIDParse(_) => "UUID Parse Error",
            Error::General(ref err) => err.as_str(),
        }
    }
}

impl From<io::Error> for Error {
    fn from(err: io::Error) -> Error {
        Error::Io(err)
    }
}

impl From<CDRSError> for Error {
    fn from(err: CDRSError) -> Error {
        Error::Server(err)
    }
}

impl From<FromUtf8Error> for Error {
    fn from(err: FromUtf8Error) -> Error {
        Error::FromUtf8(err)
    }
}

impl From<UUIDError> for Error {
    fn from(err: UUIDError) -> Error {
        Error::UUIDParse(err)
    }
}

impl From<String> for Error {
    fn from(err: String) -> Error {
        Error::General(err)
    }
}

impl<'a> From<&'a str> for Error {
    fn from(err: &str) -> Error {
        Error::General(err.to_string())
    }
}

#[cfg(tests)]
mod test {
    use super::*;

    #[test]
    fn column_is_empty_err() {
        let err = column_is_empty_err("some_column");

        if let Error::General(err_description) = err {
            assert_eq!(
                err_description,
                "Column or UDT property 'some_column' is empty"
            );
            return;
        }

        panic!("Unexpected result of column_is_empty_err, {:?}", err);
    }

    #[test]
    fn error_display() {
        // TODO:
    }
}
