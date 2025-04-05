use dashmap::DashMap;
use lasso::{Spur, ThreadedRodeo};
use once_cell::sync::Lazy;
use serde::{Deserialize, Serialize, Serializer};
use std::{
    borrow::Cow,
    sync::atomic::{AtomicUsize, Ordering},
};

pub const INTERN_THRESHOLD: usize = 16;

struct Counter {
    map: DashMap<String, AtomicUsize>,
}

impl Counter {
    fn new() -> Self {
        Self {
            map: DashMap::new(),
        }
    }

    // 增加字符串引用计数，返回当前计数
    fn increment<T: AsRef<str>>(&self, s: T) -> usize {
        let s = s.as_ref().to_string();
        let entry = self.map.entry(s).or_insert_with(|| AtomicUsize::new(0));
        entry.fetch_add(1, Ordering::SeqCst) + 1
    }

    // 获取字符串的引用计数
    fn get_count<T: AsRef<str>>(&self, s: T) -> usize {
        self.map
            .get(s.as_ref())
            .map(|count| count.load(Ordering::SeqCst))
            .unwrap_or(0)
    }
}

pub struct Interner(ThreadedRodeo<Spur>);

// 字符串可以有两种形式：已 intern 的符号或普通字符串
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Symbol {
    Interned(lasso::Spur),
    String(String),
}

static GLOBAL_COUNTER: Lazy<Counter> = Lazy::new(|| Counter::new());
pub static GLOBAL_INTERNER: Lazy<Interner> = Lazy::new(|| Interner::new());

impl Interner {
    fn new() -> Self {
        Self(ThreadedRodeo::new())
    }

    pub fn get_or_intern<T>(&self, s: T) -> Symbol
    where
        T: AsRef<str>,
    {
        let s_ref = s.as_ref();
        let count = GLOBAL_COUNTER.increment(s_ref);

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

pub fn resolve(symbol: &Symbol) -> Cow<'static, str> {
    GLOBAL_INTERNER.resolve(symbol)
}

pub fn intern<T>(s: T) -> Symbol
where
    T: AsRef<str>,
{
    GLOBAL_INTERNER.get_or_intern(s)
}

impl Symbol {
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
            let spur = GLOBAL_INTERNER.inner().get_or_intern(s);
            *self = Symbol::Interned(spur);
        }
    }
}

impl std::fmt::Display for Symbol {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Symbol::Interned(spur) => {
                let str = GLOBAL_INTERNER.resolve(&Symbol::Interned(*spur));
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
            Symbol::Interned(spur) => GLOBAL_INTERNER.0.resolve(spur),
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
                let str = GLOBAL_INTERNER.0.resolve(spur);
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
