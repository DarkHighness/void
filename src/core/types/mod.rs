mod error;
mod record;
mod string;
mod r#type;
mod value;

pub use error::{Error, Result};
pub use r#type::DataType;
pub use record::{
    Attribute, Record, STAGE_INBOUND_RECEIVED, STAGE_OUTBOUND_PROCESSED, STAGE_OUTBOUND_RECEIVED,
    STAGE_PIPE_PROCESSED, STAGE_PIPE_RECEIVED,
};
#[allow(unused_imports)]
pub use string::{intern, num_interned_strings, resolve, Symbol};
pub use value::{parse_value, Value};
