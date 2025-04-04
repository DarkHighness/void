mod error;
mod record;
mod string;
mod r#type;
mod value;

pub use error::{Error, Result};
pub use r#type::DataType;
pub use record::Record;
pub use string::{intern, resolve, Symbol};
pub use value::{parse_value, Value};
