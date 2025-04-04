use lasso::{Spur, ThreadedRodeo};
use serde::{Deserialize, Serialize, Serializer};

pub struct Interner(ThreadedRodeo<Spur>);

#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
pub struct Symbol {
    inner: lasso::Spur,
}

impl Interner {
    fn new() -> Self {
        Self(ThreadedRodeo::new())
    }

    pub fn get_or_intern<T>(&self, s: T) -> Symbol
    where
        T: AsRef<str>,
    {
        let symbol = self.0.get_or_intern(s);
        symbol.into()
    }

    pub fn resolve(&self, symbol: Symbol) -> &str {
        self.0.resolve(&symbol.inner)
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    fn internal(&self) -> &ThreadedRodeo<Spur> {
        &self.0
    }
}

pub static INTERNER: once_cell::sync::Lazy<Interner> =
    once_cell::sync::Lazy::new(|| Interner::new());

pub fn resolve(symbol: Symbol) -> &'static str {
    INTERNER.resolve(symbol)
}

pub fn intern<T>(s: T) -> Symbol
where
    T: AsRef<str>,
{
    INTERNER.get_or_intern(s)
}

impl Symbol {
    pub fn is_empty(&self) -> bool {
        self.as_ref().is_empty()
    }
}

impl std::fmt::Display for Symbol {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let str = INTERNER.resolve(*self);
        write!(f, "{}", str)
    }
}

impl From<lasso::Spur> for Symbol {
    fn from(symbol: lasso::Spur) -> Self {
        Self { inner: symbol }
    }
}

impl From<&str> for Symbol {
    fn from(s: &str) -> Self {
        INTERNER.get_or_intern(s)
    }
}

impl From<String> for Symbol {
    fn from(s: String) -> Self {
        INTERNER.get_or_intern(s)
    }
}

impl AsRef<str> for Symbol {
    fn as_ref(&self) -> &str {
        INTERNER.resolve(*self)
    }
}

impl Serialize for Symbol {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let str = INTERNER.resolve(*self);
        serializer.serialize_str(&str)
    }
}

impl<'de> Deserialize<'de> for Symbol {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        Ok(INTERNER.get_or_intern(s))
    }
}
