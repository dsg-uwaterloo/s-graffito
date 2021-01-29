use std::collections::{HashMap, HashSet};

/// DFA implementation where each transition is deterministic, i.e., there is at most one target node for each transition
#[derive(Debug, Clone)]
pub struct DFA {
    pub num_states: u8,
    pub final_states: HashSet<u8>,
    transitions: HashMap<String, HashSet<(u8, u8)>>,
    forward_transitions: Vec<Vec<(String, u8)>>,
    backward_transitions: Vec<Vec<(String, u8)>>,
    pub alphabet: HashSet<String>,
}

impl DFA {
    pub fn new(num_states: u8, final_states: HashSet<u8>) -> Self {
        Self {
            num_states,
            final_states,
            transitions: HashMap::new(),
            forward_transitions: vec![Vec::new(); num_states as usize],
            backward_transitions: vec![Vec::new(); num_states as usize],
            alphabet: HashSet::new(),
        }
    }


    /// Updates the transition graph of the automata
    pub fn add_transition(&mut self, source_state: u8, target_state: u8, label: String) {
        // update alphabet
        self.alphabet.insert(label.clone());

        // add label transition
        self.transitions.entry(label.clone()).or_insert(HashSet::new()).insert((source_state, target_state));

        // add forward transition
        if let Some(_position) = self.forward_transitions[source_state as usize].iter().position(|(l, _target)| l == &label) {
            // this should not happen
        } else {
            self.forward_transitions[source_state as usize].push((label.clone(), target_state))
        }

        // add backward transition
        if let Some(_position) = self.backward_transitions[target_state as usize].iter().position(|(l, _source)| l == &label) {
            // this should not happen
        } else {
            self.backward_transitions[target_state as usize].push((label.clone(), source_state))
        }
    }

    /// Given a label in the alphabet, return an iterator of state-pairs that corresponds to given label
    pub fn get_transitions(&self, label: &str) -> Vec<(u8, u8)> {
        self.transitions.get(label).iter().flat_map(|transition| transition.iter()).cloned().collect()
    }

    /// Given a state, retrieve all label-state pairs (transitions) that originates from the given state
    pub fn get_outgoing_transitions(&self, state: u8) -> Vec<(String, u8)> {
        self.forward_transitions[state as usize].iter().cloned().collect()
    }

    /// Retrieve all label-state pairs (transitions) that leads to given state
    pub fn get_incoming_transitions(&self, state: u8) -> Vec<(String, u8)> {
        self.backward_transitions[state as usize].iter().cloned().collect()
    }

    pub fn state_move(&self, state: u8, label: &str) -> Option<u8> {
        self.forward_transitions[state as usize].iter()
            .find(|(l, _target)| l == label)
            .map(|(_l, target)| *target)
    }

    /// Returns true if given state is a final state of the automata
    pub fn is_final_state(&self, state: u8) -> bool {
        self.final_states.contains(&state)
    }

    /// Returns true if given label is a part of the alphabet
    pub fn contains_label(&self, label: &str) -> bool {
        self.alphabet.contains(label)
    }

    /// Given a label, return its reference in the alphabet if the label is in the alphabet of this NFA
    pub fn get_label(&self, label: String) -> Option<&String> {
        self.alphabet.get(&label)
    }

    /// Returns true if given word, i.e, a vector of alphabet characters
    /// panics if given word has characters that are not part of the alphabet
    pub fn accept(&self, word: Vec<&str>) -> bool {
        // panic if a character is not a part of the alphabet
        assert!(word.iter().all(|character| self.contains_label(character)));

        let mut current_state = 0;

        for label in word {
            if let Some(next_state) = self.state_move(current_state, label) {
                current_state = next_state;
            } else {
                // word is not accepted
                return false;
            }
        }

        // word is accepted if automata is in a final state
        self.is_final_state(current_state)
    }
}
