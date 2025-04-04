use std::{collections::HashMap, sync::Arc};

use super::{Symbol, Value};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Attribute {}

#[derive(Debug, Clone)]
pub struct Record {
    values: HashMap<Symbol, Value>,

    attributes: HashMap<Attribute, Value>,
}

impl Record {
    pub fn empty() -> Self {
        Self {
            values: HashMap::new(),
            attributes: HashMap::new(),
        }
    }

    pub fn new(values: HashMap<Symbol, Value>) -> Self {
        Self {
            values,
            attributes: HashMap::new(),
        }
    }

    pub fn new_with_attributes(
        values: HashMap<Symbol, Value>,
        attributes: HashMap<Attribute, Value>,
    ) -> Self {
        Self { values, attributes }
    }

    pub fn set(&mut self, key: Symbol, value: Value) {
        self.values.insert(key, value);
    }

    pub fn get(&self, key: &Symbol) -> Option<&Value> {
        self.values.get(key)
    }

    pub fn get_mut(&mut self, key: &Symbol) -> Option<&mut Value> {
        self.values.get_mut(key)
    }

    pub fn set_attribute_overwrite(&mut self, key: Attribute, value: Value, overwrite: bool) {
        if overwrite {
            self.attributes.insert(key, value);
        } else {
            self.attributes.entry(key).or_insert(value);
        }
    }

    pub fn set_attribute(&mut self, key: Attribute, value: Value) {
        self.set_attribute_overwrite(key, value, false);
    }

    pub fn get_attribute(&self, key: &Attribute) -> Option<&Value> {
        self.attributes.get(key)
    }

    pub fn get_attribute_mut(&mut self, key: &Attribute) -> Option<&mut Value> {
        self.attributes.get_mut(key)
    }

    pub fn take(self) -> HashMap<Symbol, Value> {
        self.values
    }
}

impl From<HashMap<Symbol, Value>> for Record {
    fn from(values: HashMap<Symbol, Value>) -> Self {
        Self::new(values)
    }
}

impl FromIterator<(Symbol, Value)> for Record {
    fn from_iter<T: IntoIterator<Item = (Symbol, Value)>>(iter: T) -> Self {
        let mut values = HashMap::new();
        for (key, value) in iter {
            values.insert(key, value);
        }
        Self::new(values)
    }
}

impl std::ops::Index<&Symbol> for Record {
    type Output = Value;

    fn index(&self, index: &Symbol) -> &Self::Output {
        self.values.get(index).unwrap()
    }
}

impl std::ops::Deref for Record {
    type Target = HashMap<Symbol, Value>;

    fn deref(&self) -> &Self::Target {
        &self.values
    }
}
