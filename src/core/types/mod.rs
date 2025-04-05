mod error;
mod record;
mod string;
mod r#type;
mod value;

pub use error::{Error, Result};
pub use r#type::DataType;
pub use record::{Attribute, Record};
pub use string::{resolve, Symbol};
pub use value::{parse_value, Value};
