use dashmap::DashMap;
use lasso::{Spur, ThreadedRodeo};
use once_cell::sync::Lazy;
use serde::{Deserialize, Serialize, Serializer};
use std::{
    borrow::Cow,
    sync::atomic::{AtomicUsize, Ordering},
};

pub const INTERN_THRESHOLD: usize = 8;

struct Counter {
    map: DashMap<String, AtomicUsize>,
}

impl Counter {
    fn new() -> Self {
        Self {
            map: DashMap::new(),
        }
    }

    fn increment<T: AsRef<str>>(&self, s: T) -> usize {
        let s = s.as_ref().to_string();
        let entry = self.map.entry(s).or_insert_with(|| AtomicUsize::new(0));
        entry.fetch_add(1, Ordering::SeqCst) + 1
    }

    fn get_count<T: AsRef<str>>(&self, s: T) -> usize {
        self.map
            .get(s.as_ref())
            .map(|count| count.load(Ordering::SeqCst))
            .unwrap_or(0)
    }
}

pub struct Interner(ThreadedRodeo<Spur>);

#[derive(Debug)]
pub enum Symbol {
    Interned(lasso::Spur),
    String(String),
}

impl Symbol {
    pub fn new<T>(s: T) -> Self
    where
        T: AsRef<str>,
    {
        INTERNER.get_or_intern(s)
    }
}

impl PartialOrd for Symbol {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        match (self, other) {
            (Symbol::Interned(spur1), Symbol::Interned(spur2)) => spur1.partial_cmp(spur2),
            (Symbol::String(s1), Symbol::String(s2)) => s1.partial_cmp(s2),
            (Symbol::Interned(spur), Symbol::String(s)) => {
                let str = INTERNER.resolve(&Symbol::Interned(*spur));
                str.as_bytes().partial_cmp(s.as_bytes())
            }
            (Symbol::String(s), Symbol::Interned(spur)) => {
                let str = INTERNER.resolve(&Symbol::Interned(*spur));
                s.as_bytes().partial_cmp(str.as_bytes())
            }
        }
    }
}

impl PartialEq for Symbol {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Symbol::Interned(spur1), Symbol::Interned(spur2)) => spur1 == spur2,
            (Symbol::String(s1), Symbol::String(s2)) => s1 == s2,
            (Symbol::Interned(spur), Symbol::String(s)) => {
                let str = INTERNER.resolve(&Symbol::Interned(*spur));
                str.as_bytes() == s.as_bytes()
            }
            (Symbol::String(s), Symbol::Interned(spur)) => {
                let str = INTERNER.resolve(&Symbol::Interned(*spur));
                s.as_bytes() == str.as_bytes()
            }
        }
    }
}

impl Eq for Symbol {}

impl std::hash::Hash for Symbol {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        match self {
            Symbol::Interned(spur) => {
                let str = INTERNER.resolve(&Symbol::Interned(*spur));
                str.hash(state);
            }
            Symbol::String(s) => s.hash(state),
        }
    }
}

static INTERNER_COUNTER: Lazy<Counter> = Lazy::new(|| Counter::new());
pub static INTERNER: Lazy<Interner> = Lazy::new(|| Interner::new());

impl Interner {
    fn new() -> Self {
        Self(ThreadedRodeo::new())
    }

    pub fn get_or_intern<T>(&self, s: T) -> Symbol
    where
        T: AsRef<str>,
    {
        let s_ref = s.as_ref();
        let count = INTERNER_COUNTER.increment(s_ref);

        if count >= INTERN_THRESHOLD {
            let symbol = self.0.get_or_intern(s_ref);
            Symbol::Interned(symbol)
        } else {
            Symbol::String(s_ref.to_string())
        }
    }

    pub fn resolve(&self, symbol: &Symbol) -> Cow<'_, str> {
        match symbol {
            Symbol::Interned(spur) => Cow::Borrowed(self.0.resolve(spur)),
            Symbol::String(s) => Cow::Owned(s.clone()),
        }
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    fn inner(&self) -> &ThreadedRodeo<Spur> {
        &self.0
    }
}

impl Symbol {
    pub fn intern<T>(str: T) -> Self
    where
        T: AsRef<str>,
    {
        let s = str.as_ref();
        let _ = INTERNER_COUNTER.increment(s);
        let supr = INTERNER.inner().get_or_intern(s);
        Symbol::Interned(supr)
    }

    pub fn is_empty(&self) -> bool {
        match self {
            Symbol::Interned(_) => resolve(self).is_empty(),
            Symbol::String(s) => s.is_empty(),
        }
    }

    pub fn is_interned(&self) -> bool {
        matches!(self, Symbol::Interned(_))
    }

    pub fn force_intern(&mut self) {
        if let Symbol::String(s) = self {
            let _ = INTERNER_COUNTER.increment(&s);
            let spur = INTERNER.inner().get_or_intern(s);
            *self = Symbol::Interned(spur);
        }
    }
}

impl Clone for Symbol {
    fn clone(&self) -> Self {
        match self {
            Symbol::Interned(spur) => Symbol::Interned(*spur),
            Symbol::String(s) => {
                let count = INTERNER_COUNTER.increment(&s);
                if count >= INTERN_THRESHOLD {
                    let spur = INTERNER.inner().get_or_intern(s);
                    Symbol::Interned(spur)
                } else {
                    Symbol::String(s.clone())
                }
            }
        }
    }
}

impl std::fmt::Display for Symbol {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Symbol::Interned(spur) => {
                let str = INTERNER.resolve(&Symbol::Interned(*spur));
                write!(f, "{}", str)
            }
            Symbol::String(s) => write!(f, "{}", s),
        }
    }
}

impl From<lasso::Spur> for Symbol {
    fn from(symbol: lasso::Spur) -> Self {
        Symbol::Interned(symbol)
    }
}

impl From<&str> for Symbol {
    fn from(s: &str) -> Self {
        intern(s)
    }
}

impl From<String> for Symbol {
    fn from(s: String) -> Self {
        intern(s)
    }
}

impl AsRef<str> for Symbol {
    fn as_ref(&self) -> &str {
        match self {
            Symbol::Interned(spur) => INTERNER.0.resolve(spur),
            Symbol::String(s) => s.as_str(),
        }
    }
}

impl Serialize for Symbol {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            Symbol::Interned(spur) => {
                let str = INTERNER.0.resolve(spur);
                serializer.serialize_str(str)
            }
            Symbol::String(s) => serializer.serialize_str(s),
        }
    }
}

impl<'de> Deserialize<'de> for Symbol {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        Ok(intern(s))
    }
}

pub fn resolve(symbol: &Symbol) -> Cow<'static, str> {
    INTERNER.resolve(symbol)
}

pub fn intern<T>(s: T) -> Symbol
where
    T: AsRef<str>,
{
    INTERNER.get_or_intern(s)
}

pub fn num_interned_strings() -> usize {
    INTERNER.len()
}
