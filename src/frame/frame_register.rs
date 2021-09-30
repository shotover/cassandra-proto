use rand;

use crate::error;
use crate::frame::events::SimpleServerEvent;
use crate::frame::*;
use crate::types::{CString, CStringList};
use std::io::Cursor;

/// The structure which represents a body of a frame of type `options`.
#[derive(Debug)]
pub struct BodyReqRegister {
    pub events: Vec<SimpleServerEvent>,
}

impl IntoBytes for BodyReqRegister {
    fn into_cbytes(&self) -> Vec<u8> {
        let events_string_list = CStringList {
            list: self
                .events
                .iter()
                .map(|event| CString::new(event.as_string()))
                .collect(),
        };
        events_string_list.into_cbytes()
    }
}

impl FromCursor for BodyReqRegister {
    fn from_cursor(cursor: &mut Cursor<&[u8]>) -> error::Result<BodyReqRegister> {
        let list = CStringList::from_cursor(cursor)?;
        Ok(BodyReqRegister {
            events: list
                .into_plain()
                .iter()
                .map(|x| match x.as_str() {
                    events::TOPOLOGY_CHANGE => SimpleServerEvent::TopologyChange,
                    events::STATUS_CHANGE => SimpleServerEvent::StatusChange,
                    events::SCHEMA_CHANGE => SimpleServerEvent::SchemaChange,
                    &_ => unreachable!(),
                })
                .collect(),
        })
    }
}

// Frame implementation related to BodyReqRegister

impl Frame {
    /// Creates new frame of type `REGISTER`.
    pub fn new_req_register(events: Vec<SimpleServerEvent>) -> Frame {
        let version = Version::Request;
        let flag = Flag::Ignore;
        let stream = rand::random::<u16>();
        let opcode = Opcode::Register;
        let register_body = BodyReqRegister { events: events };

        Frame {
            version: version,
            flags: vec![flag],
            stream: stream,
            opcode: opcode,
            body: register_body.into_cbytes(),
            // for request frames it's always None
            tracing_id: None,
            warnings: vec![],
        }
    }
}
