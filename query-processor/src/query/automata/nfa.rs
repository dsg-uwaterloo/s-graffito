use std::collections::{HashMap, HashSet, VecDeque};
use std::iter::FromIterator;

/// Non-deterministic finite automata implementation
#[derive(Debug, Clone)]
pub struct NFA {
    pub num_states: u8,
    pub final_states: HashSet<u8>,
    transitions: HashMap<String, HashSet<(u8, u8)>>,
    forward_transitions: Vec<Vec<(String, Vec<u8>)>>,
    backward_transitions: Vec<Vec<(String, Vec<u8>)>>,
    epsilon_transitions: Vec<Vec<u8>>,
    pub alphabet: HashSet<String>,
}

impl NFA {
    pub fn new(num_states: u8, final_states: HashSet<u8>) -> Self {
        Self {
            num_states,
            final_states,
            transitions: HashMap::new(),
            forward_transitions: vec![Vec::new(); num_states as usize],
            backward_transitions: vec![Vec::new(); num_states as usize],
            epsilon_transitions: vec![Vec::new(); num_states as usize],
            alphabet: HashSet::new(),
        }
    }

    /// adds an epsilon transition to the automata
    pub fn add_epsilon_transition(&mut self, source_state: u8, target_state: u8) {
        self.epsilon_transitions[source_state as usize].push(target_state);
    }

    /// Updates the transition graph of the automata
    pub fn add_transition(&mut self, source_state: u8, target_state: u8, label: String) {
        // update alphabet
        self.alphabet.insert(label.clone());

        // add label transition
        self.transitions.entry(label.clone()).or_insert(HashSet::new()).insert((source_state, target_state));

        // add forward transition
        if let Some(position) = self.forward_transitions[source_state as usize].iter().position(|(l, _targets)| l == &label) {
            // label exists for that state
            let targets = &mut self.forward_transitions[source_state as usize][position].1;
            if targets.iter().all(|t| *t != target_state) {
                // insert target if it is not already there
                targets.push(target_state)
            }
        } else {
            let mut targets = Vec::new();
            targets.push(target_state);
            self.forward_transitions[source_state as usize].push((label.clone(), targets));
        }

        // add backward transition
        if let Some(position) = self.backward_transitions[target_state as usize].iter().position(|(l, _sources)| l == &label) {
            // label exists for that state
            let sources = &mut self.backward_transitions[target_state as usize][position].1;
            if sources.iter().all(|s| *s != source_state) {
                // insert target if it is not already there
                sources.push(source_state)
            }
        } else {
            let mut sources = Vec::new();
            sources.push(source_state);
            self.backward_transitions[target_state as usize].push((label.clone(), sources));
        }
    }

    /// Given a label in the alphabet, return an iterator of state-pairs that corresponds to given label
    pub fn get_transitions(&self, label: &str) -> Vec<(u8, u8)> {
        self.transitions.get(label).iter().flat_map(|transition| transition.iter()).cloned().collect()
    }

    pub fn get_epsilon_transitions(&self, state: u8) -> Vec<u8> {
        self.epsilon_transitions[state as usize].iter().cloned().collect()
    }

    /// Given a state, retrieve all label-state pairs (transitions) that originates from the given state
    pub fn get_outgoing_transitions(&self, state: u8) -> Vec<(String, Vec<u8>)> {
        self.forward_transitions[state as usize].iter().cloned().collect()
    }

    /// Retrieve all label-state pairs (transitions) that leads to given state
    pub fn get_incoming_transitions(&self, state: u8) -> Vec<(String, Vec<u8>)> {
        self.backward_transitions[state as usize].iter().cloned().collect()
    }

    pub fn state_move(&self, state: u8, label: &str) -> Option<Vec<u8>> {
        self.forward_transitions[state as usize].iter()
            .find(|(l, _targets)| l == label)
            .map(|(_l, targets)| targets).cloned()
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


        let mut closure = HashSet::<u8>::from_iter(self.get_epsilon_closure(0).into_iter());


        for label in word {
            // obtain e-closure of the current states
            let mut next_states = HashSet::new();

            // check all states that can be reached with move label
            for state in closure.drain() {
                if let Some(moves) = self.state_move(state, label) {
                    moves.iter().for_each(|state| {
                        next_states.insert(*state);
                    });
                }
            }

            if next_states.is_empty() {
                // there is no valid transition
                return false;
            } else {
                next_states.drain()
                    .flat_map(|state| self.get_epsilon_closure(state).into_iter())
                    .for_each(|e_state| {
                        closure.insert(e_state);
                    });
            }
        }

        // word is accepted if there is a final state
        closure.into_iter().any(|state| self.is_final_state(
            state))
    }


    /// retrieve epsilon-closure of the given state
    pub fn get_epsilon_closure(&self, state: u8) -> HashSet<u8> {
        let mut e_closure = HashSet::new();
        let mut queue = VecDeque::new();

        // start with the source state
        queue.push_back(state);

        // find all reachable nodes
        while let Some(current_state) = queue.pop_front() {
            // current state is reachable
            e_closure.insert(current_state);

            // traverse its neighbours
            self.get_epsilon_transitions(current_state).iter().filter(|neighbour| !e_closure.contains(neighbour))
                .for_each(|neighbour| queue.push_back(*neighbour));
        }

        // return epsilon closure of the state
        e_closure
    }
}