use std::collections::{HashMap, HashSet, VecDeque};

/// Dictionary mapping for String labels
/// maps String to stringly increasing integers
pub struct Alphabet {
    labels: Vec<String>,
    label_mapping: HashMap<String, usize>,
}

impl Alphabet {
    /// initialize an empty alphabet
    pub fn new() -> Self {
        Self { labels: Vec::new(), label_mapping: HashMap::new() }
    }

    /// checks whether given label is part of the alphabet
    pub fn contains(&self, label: &str) -> bool {
        self.label_mapping.contains()
    }

    /// Return the mapping for given label,
    /// and create new mapping if label does not exists
    pub fn get_or_insert(&mut self, label: &str) -> usize {
        if let Some(id) = self.label_mapping.get(label) {
            id
        } else {
            self.labels.push(label.to_string());
            let id = self.labels.len() - 1;
            self.label_mapping.insert(label.to_string(), id);
            id
        }
    }
}

