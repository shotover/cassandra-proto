use crate::consistency::Consistency;
use crate::types::{to_bigint, to_int, to_short, CBytes, CString, cursor_next_value};
use crate::frame::{AsByte, FromCursor};
use crate::frame::IntoBytes;
use super::query_flags::QueryFlags;
use super::query_values::QueryValues;
use std::io::Cursor;
use bytes::Buf;
use std::collections::HashMap;
use crate::types::value::{Value, ValueType};
use crate::error::Error;

/// Parameters of Query for query operation.
#[derive(Debug, Default)]
pub struct QueryParams {
  /// Cassandra consistency level.
  pub consistency: Consistency,
  /// Array of query flags.
  pub flags: Vec<QueryFlags>,
  /// Were values provided with names
  pub with_names: Option<bool>,
  /// Array of values.
  pub values: Option<QueryValues>,
  /// Page size.
  pub page_size: Option<i32>,
  /// Array of bytes which represents paging state.
  pub paging_state: Option<CBytes>,
  /// Serial `Consistency`.
  pub serial_consistency: Option<Consistency>,
  /// Timestamp.
  pub timestamp: Option<i64>,
}

impl QueryParams {
  /// Sets values of Query request params.
  pub fn set_values(&mut self, values: QueryValues) {
    self.flags.push(QueryFlags::Value);
    self.values = Some(values);
  }

  fn flags_as_byte(&self) -> u8 {
    self.flags.iter().fold(0, |acc, flag| acc | flag.as_byte())
  }

  #[allow(dead_code)]
  pub fn parse_query_flags(byte: u8) -> Vec<QueryFlags> {
    let mut flags: Vec<QueryFlags> = vec![];

    if QueryFlags::has_value(byte) {
      flags.push(QueryFlags::Value);
    }
    if QueryFlags::has_skip_metadata(byte) {
      flags.push(QueryFlags::SkipMetadata);
    }
    if QueryFlags::has_page_size(byte) {
      flags.push(QueryFlags::PageSize);
    }
    if QueryFlags::has_with_paging_state(byte) {
      flags.push(QueryFlags::WithPagingState);
    }
    if QueryFlags::has_with_serial_consistency(byte) {
      flags.push(QueryFlags::WithSerialConsistency);
    }
    if QueryFlags::has_with_default_timestamp(byte) {
      flags.push(QueryFlags::WithDefaultTimestamp);
    }
    if QueryFlags::has_with_names_for_values(byte) {
      flags.push(QueryFlags::WithNamesForValues);
    }

    flags
  }
}

impl FromCursor for QueryParams {
  fn from_cursor(cursor: &mut Cursor<&[u8]>) -> Result<QueryParams, Error> {
    let consistency = Consistency::from_cursor(cursor)?;
    let flags_byte = cursor.get_u8();
    let flags: Vec<QueryFlags> = QueryParams::parse_query_flags(flags_byte);
    let mut q_values: Option<QueryValues> = None;

    if QueryFlags::has_value(flags_byte) {
      let mut map: HashMap<String, Value> = HashMap::new();
      let mut vec: Vec<Value> = Vec::new();
      if QueryFlags::has_with_names_for_values(flags_byte) {
        let number_of_values = cursor.get_u8();
        for _i in 1.. number_of_values {
          let name = CString::from_cursor(cursor)?;
          let value_size = cursor.get_i32();
          let val_type: Value;
          if value_size > 0 {
            val_type = Value::new_normal(cursor_next_value(cursor, value_size as u64)?)
          } else if value_size == -1 {
            val_type = Value::new_null();
          } else if value_size == -2 {
            val_type = Value::new_not_set();
          } else {
            return Err(Error::General(String::from("Could not decode query values")));
          }
          map.insert(name.as_plain(), val_type);
        }
        q_values = Some(QueryValues::NamedValues(map.clone()));
      } else {
        let number_of_values = cursor.get_u8();
        for _i in 1.. number_of_values {
          let value_size = cursor.get_i32();
          let val_type: Value;
          if value_size > 0 {
            let body = cursor_next_value(cursor, value_size as u64)?;
            let l = body.len() as i32;
            val_type = Value {
              body,
              value_type: ValueType::Normal(l)
            }
          } else if value_size == -1 {
            val_type = Value::new_null();
          } else if value_size == -2 {
            val_type = Value::new_not_set();
          } else {
            return Err(Error::General(String::from("Could not decode query values")));
          }
          vec.push(val_type);
        }
        q_values = Some(QueryValues::SimpleValues(vec));
      }


    }

    let mut page_size = None;
    if QueryFlags::has_page_size(flags_byte) {
      page_size = Some(cursor.get_i32());
    }
    let mut page_state = None;
    if QueryFlags::has_with_paging_state(flags_byte) {
      page_state = Some(CBytes::from_cursor(cursor)?);
    }

    let mut serial_consistency = None;
    if QueryFlags::has_with_serial_consistency(flags_byte) {
      serial_consistency = Some(Consistency::from_cursor(cursor)?);
    }

    let mut timestamp = None;
    if QueryFlags::has_with_default_timestamp(flags_byte) {
      timestamp = Some(cursor.get_i64());
    }

    Ok(QueryParams{
      consistency,
      flags,
      with_names: Some(QueryFlags::has_with_names_for_values(flags_byte)),
      values: q_values,
      page_size,
      paging_state: page_state,
      serial_consistency,
      timestamp
    })
  }
}

impl IntoBytes for QueryParams {
  fn into_cbytes(&self) -> Vec<u8> {
    let mut v: Vec<u8> = vec![];

    v.extend_from_slice(self.consistency.into_cbytes().as_slice());
    v.push(self.flags_as_byte());
    if QueryFlags::has_value(self.flags_as_byte()) {
      if let Some(ref values) = self.values {
        v.extend_from_slice(to_short(values.len() as i16).as_slice());
        v.extend_from_slice(values.into_cbytes().as_slice());
      }
    }
    if QueryFlags::has_page_size(self.flags_as_byte()) && self.page_size.is_some() {
      // XXX clone
      v.extend_from_slice(to_int(self.page_size
                                    .clone()
                                    // unwrap is safe as we've checked that
                                    // self.page_size.is_some()
                                    .unwrap())
                                    .as_slice());
    }
    if QueryFlags::has_with_paging_state(self.flags_as_byte()) && self.paging_state.is_some() {
      // XXX clone
      v.extend_from_slice(self.paging_state
                                    .clone()
                                    // unwrap is safe as we've checked that
                                    // self.paging_state.is_some()
                                    .unwrap()
                                    .into_cbytes()
                                    .as_slice());
    }
    if QueryFlags::has_with_serial_consistency(self.flags_as_byte())
       && self.serial_consistency.is_some() {
      // XXX clone
      v.extend_from_slice(self.serial_consistency
                                    .clone()
                                    // unwrap is safe as we've checked that
                                    // self.serial_consistency.is_some()
                                    .unwrap()
                                    .into_cbytes()
                                    .as_slice());
    }
    if QueryFlags::has_with_default_timestamp(self.flags_as_byte()) && self.timestamp.is_some() {
      // unwrap is safe as we've checked that self.timestamp.is_some()
      v.extend_from_slice(to_bigint(self.timestamp.unwrap()).as_slice());
    }

    v
  }
}
