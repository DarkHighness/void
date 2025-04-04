use spin::Mutex;

use serde::{Deserialize, Serialize, Serializer};
use string_interner::{DefaultBackend, DefaultSymbol, StringInterner};

pub struct Interner(Mutex<string_interner::StringInterner<DefaultBackend>>);

#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
pub struct Symbol {
    inner: string_interner::DefaultSymbol,
    empty: bool,
}

impl Symbol {
    pub fn is_empty(&self) -> bool {
        return self.empty;
    }
}

impl Interner {
    fn new() -> Self {
        Self(Mutex::new(StringInterner::new()))
    }

    fn raw_get(&self, sym: DefaultSymbol) -> Option<String> {
        let interner = self.0.lock();
        interner.resolve(sym).map(|s| s.to_string())
    }

    pub fn get<T>(&self, s: T) -> Option<Symbol>
    where
        T: AsRef<str>,
    {
        let interner = self.0.lock();
        interner.get(s).map(|symbol| Symbol::from(symbol))
    }

    pub fn get_or_intern<T>(&self, s: T) -> Symbol
    where
        T: AsRef<str>,
    {
        let mut interner = self.0.lock();
        let symbol = interner.get_or_intern(s);
        symbol.into()
    }

    pub fn resolve(&self, symbol: Symbol) -> Option<String> {
        let interner = self.0.lock();
        interner.resolve(symbol.inner).map(|s| s.to_string())
    }

    pub fn len(&self) -> usize {
        let interner = self.0.lock();
        interner.len()
    }

    pub fn is_empty(&self) -> bool {
        let interner = self.0.lock();
        interner.is_empty()
    }
}

pub static INTERNER: once_cell::sync::Lazy<Interner> =
    once_cell::sync::Lazy::new(|| Interner::new());

pub fn intern<T>(s: T) -> Symbol
where
    T: AsRef<str>,
{
    INTERNER.get_or_intern(s)
}

pub fn resolve(symbol: Symbol) -> Option<String> {
    INTERNER.resolve(symbol)
}

pub fn get_symbol<T>(s: T) -> Option<Symbol>
where
    T: AsRef<str>,
{
    INTERNER.get(s)
}

impl std::fmt::Display for Symbol {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let str = INTERNER.resolve(*self).unwrap_or_default();
        write!(f, "{}", str)
    }
}

impl From<string_interner::DefaultSymbol> for Symbol {
    fn from(symbol: string_interner::DefaultSymbol) -> Self {
        let string = INTERNER.raw_get(symbol);
        let empty = string.is_none() || string.unwrap().is_empty();

        Self {
            inner: symbol,
            empty,
        }
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

impl Serialize for Symbol {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let str = INTERNER.resolve(*self).unwrap_or_default();
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
