use std::{collections::HashMap, sync::Arc};

use super::Value;

pub type Record = HashMap<Arc<str>, Value>;
