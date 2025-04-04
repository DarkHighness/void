pub mod r#type;

pub enum Auth {
    None,
    Basic { username: String, password: String },
    Bearer { token: String },
}

pub struct PrometheusOutbound {
    address: String,
}
