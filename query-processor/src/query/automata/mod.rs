use std::cmp::{max, min};
use std::collections::{BTreeSet, HashMap, HashSet, VecDeque};
use std::iter::FromIterator;

use itertools::Itertools;

use crate::query::automata::{dfa::DFA, nfa::NFA};

pub mod nfa;
pub mod dfa;

/// A set of helper functions to build NFA, used for NFA construction from a given regular expression
/// based on the Thompson's construction algorithm

/// create an NFA with a single transition
pub fn transition(label: String) -> NFA {
    let mut final_states = HashSet::new();
    final_states.insert(1);
    let mut automata = NFA::new(2, final_states);
    automata.add_transition(0, 1, label);

    automata
}

/// create an NFA as a concatenation of two NFAs
pub fn concatenation(lhs: NFA, rhs: NFA) -> NFA {
    // # of states of the resulting automata is the sum of two
    let num_states = lhs.num_states + rhs.num_states;

    // # of state of the rhs automata is shifted by an offset that is equal to # of state of the lhs
    let offset = lhs.num_states;
    let mut final_states = HashSet::new();
    rhs.final_states.iter().for_each(|state| {
        final_states.insert(*state + offset);
    });

    let mut result_automata = NFA::new(num_states, final_states);

    //carry over all transitions from lhs
    move_transitions(&lhs, &mut result_automata, 0);

    // carry over all transition of rhs by shifting all states by offset
    move_transitions(&rhs, &mut result_automata, offset);

    // epsilon transition to connect two NFAs
    for final_state in lhs.final_states {
        result_automata.add_epsilon_transition(final_state, lhs.num_states)
    }

    result_automata
}

/// create an NFA as an alternation of two NFAs
pub fn alternation(lhs: NFA, rhs: NFA) -> NFA {
    // # of states of the resulting automata is the sum of two, plus one new entry and exit
    let num_states = lhs.num_states + rhs.num_states + 2;

    let mut final_states = HashSet::new();
    let final_state = num_states - 1;
    final_states.insert(final_state);

    let mut result_automata = NFA::new(num_states, final_states);

    //carry over all transitions from lhs
    // state offset for lhs is 1
    let lhs_offset = 1;
    move_transitions(&lhs, &mut result_automata, lhs_offset);

    // carry over all transition of rhs by shifting all states by offset
    // state offset for lhs.num_states + 1
    let rhs_offset = lhs.num_states + 1;
    move_transitions(&rhs, &mut result_automata, rhs_offset);

    // epsilon transition from new start state to previous two
    result_automata.add_epsilon_transition(0, lhs_offset);
    result_automata.add_epsilon_transition(0, rhs_offset);

    // epsilon transitions from previous final states to new one
    lhs.final_states.iter().for_each(|target_state|
        result_automata.add_epsilon_transition(*target_state + lhs_offset, final_state)
    );
    rhs.final_states.iter().for_each(|target_state|
        result_automata.add_epsilon_transition(*target_state + rhs_offset, final_state)
    );

    result_automata
}

/// creates a Kleene star closure over given NFA
pub fn kleene_star(input: NFA) -> NFA {
    // # of states over a Kleene star closure is two more than the original one
    let num_states = input.num_states + 2;

    let mut final_states = HashSet::new();
    let final_state = num_states - 1;
    final_states.insert(final_state);

    let mut result_automata = NFA::new(num_states, final_states);

    // carry over all original transitions
    let offset = 1;
    move_transitions(&input, &mut result_automata, offset);

    // epsilon transitions of Kleene star closure
    result_automata.add_epsilon_transition(0, 1);
    result_automata.add_epsilon_transition(0, final_state);
    input.final_states.iter().for_each(|target_state| {
        result_automata.add_epsilon_transition(*target_state + offset, final_state);
        result_automata.add_epsilon_transition(*target_state + offset, 1);
    });

    result_automata
}

/// creates a Kleene plus closure over given NFA with "+" operator
pub fn kleene_plus(input: NFA) -> NFA {
    // # of states over a Kleene plus closure is two more than the original one
    let num_states = input.num_states + 2;

    let mut final_states = HashSet::new();
    let final_state = num_states - 1;
    final_states.insert(final_state);

    let mut result_automata = NFA::new(num_states, final_states);

    // carry over all original transitions
    let offset = 1;
    move_transitions(&input, &mut result_automata, offset);

    // epsilon transitions of Kleene star closure
    result_automata.add_epsilon_transition(0, 1);
    input.final_states.iter().for_each(|target_state| {
        result_automata.add_epsilon_transition(*target_state + offset, final_state);
        result_automata.add_epsilon_transition(*target_state + offset, 1);
    });

    result_automata
}

/// helper function to create a DFA from given NFA using the subset algorithm
/// https://en.wikipedia.org/wiki/Powerset_construction
pub fn determinize(input: NFA) -> DFA {
    // ste of states in the new DFA, these are constructed from
    let mut dfa_states = HashSet::new();
    //create a start state by taking e-closure of the original start state
    let start_state = BTreeSet::from_iter(input.get_epsilon_closure(0).into_iter());
    dfa_states.insert(start_state.clone());

    // transition matrix of the final DFA
    let mut transitions: HashMap<BTreeSet<u8>, HashMap<String, BTreeSet<u8>>> = HashMap::new();


    let mut state_queue = VecDeque::new();
    state_queue.push_back(start_state.clone());

    // while there are new more states to be processed
    while let Some(next_subset) = state_queue.pop_front() {
        // transitions for this state
        let state_transitions = transitions.entry(next_subset.clone()).or_insert(HashMap::new());
        // apply move for each possible input symbol
        next_subset.iter().for_each(|state| {
            input.get_outgoing_transitions(*state).iter().cloned().for_each(|(label, target_states)| {
                let reachable_states = state_transitions.entry(label).or_insert(BTreeSet::new());
                // update reachable states for given transition based on epsilon closure
                target_states.iter().for_each(|target_state| input.get_epsilon_closure(*target_state).iter().for_each(|t| {
                    reachable_states.insert(*t);
                }));
            });
        });

        // queue newly created subset for processing
        state_transitions.iter()
            .for_each(|(_label, target_states)| {
                if !dfa_states.contains(target_states) {
                    dfa_states.insert(target_states.clone());
                    state_queue.push_back(target_states.clone());
                }
            });
    }

    let nfa_final_states = BTreeSet::from_iter(input.final_states.into_iter());
    let mut dfa_final_states = HashSet::new();

    // create a mapping from subsets to consecutive integers
    let mut next_state_no = 0;
    let mut subset_mapping = HashMap::new();
    subset_mapping.insert(start_state.clone(), next_state_no);
    // check if start state should also be a final state
    if !start_state.is_disjoint(&nfa_final_states) {
        dfa_final_states.insert(next_state_no);
    }
    next_state_no += 1;


    // mark any subset that contains a final state as a final DFA state
    dfa_states.into_iter().for_each(|subset| {
        if !subset_mapping.contains_key(&subset) {
            // mark as final if it contains any of the NFA final states
            if !subset.is_disjoint(&nfa_final_states) {
                dfa_final_states.insert(next_state_no);
            }

            // map the subset of NFA states to the DFA state
            subset_mapping.insert(subset.clone(), next_state_no);
            next_state_no += 1;
        }
    });

    // create resulting automata
    let mut result_automata = DFA::new(next_state_no, dfa_final_states);

    // move transitions to new automata
    for (source_subset, subset_transitions) in transitions {
        if let Some(source_state) = subset_mapping.get(&source_subset) {
            // traverse its transitions and update DFA
            for (label, target_subset) in subset_transitions {
                if let Some(target_state) = subset_mapping.get(&target_subset) {
                    result_automata.add_transition(*source_state, *target_state, label);
                }
            }
        }
    }

    result_automata
}

/// minimizes the given DFA using Hopcroft's algorithm (https://en.wikipedia.org/wiki/DFA_minimization)
/// It relies on  equivalence classes of the Myhill–Nerode equivalence relation
/// it starts with two coarse partitions of final and non-final states and refines partitions based on transitions
/// until no further refinement is possible
pub fn minimize(input: DFA) -> DFA {
    // create initial partitions based on final and non-final states
    let final_states = BTreeSet::from_iter(input.final_states.iter().cloned());
    let non_final_states = BTreeSet::from_iter((0..input.num_states).filter(|state| !final_states.contains(state)));

    let mut next_partitions = vec![non_final_states, final_states.clone()];
    let mut partitions = Vec::new();

    // iterate over no more partition can be generated
    while partitions.len() != next_partitions.len() {
        // swap vectors
        partitions = std::mem::take(&mut next_partitions);

        // iterate over all set-states from previous partition
        for partition in &partitions {
            // create a separate partition for each state
            partition.iter().for_each(|state| {
                let mut new_partition = BTreeSet::new();
                new_partition.insert(*state);
                next_partitions.push(new_partition);
            });
            // find indistinguishable partitions if there are multiple states
            if partition.len() != 1 {
                // construct all pairs in the set
                partition.iter().cartesian_product(partition.iter())
                    .filter(|(first, second)| *first > *second).for_each(|(first, second)| {
                    // combine indistinguishable states
                    if !is_distinguishable(&input, &partitions, *first, *second) {
                        // two states are not distinguishable, so assign same partition id to both
                        let pos1 = next_partitions.iter().position(|partition| partition.contains(first)).unwrap();
                        let pos2 = next_partitions.iter().position(|partition| partition.contains(second)).unwrap();
                        // combine if they are in different partitions
                        if pos1 != pos2 {
                            next_partitions.swap_remove(max(pos1, pos2)).iter().for_each(|state| {
                                next_partitions[min(pos1, pos2)].insert(*state);
                            });
                        }
                    }
                });
            }
        }
    }

    // next partitions contains the final partitioning
    // first find the partition with the start state
    let start_partition = next_partitions.swap_remove(next_partitions.iter().position(|state_set| state_set.contains(&0)).unwrap());
    let mut minimized_dfa_final_states = HashSet::new();

    //create a mapping from subset to consecutive integers
    let mut next_state_no = 0;
    let mut state_mapping = HashMap::new();

    start_partition.iter().for_each(|start_state| {
        state_mapping.insert(*
                                 start_state, next_state_no);
    });
    // start partition is also final if any state in the partition is a final state
    if !start_partition.is_disjoint(&final_states) {
        minimized_dfa_final_states.insert(next_state_no);
    }
    next_state_no += 1;

    //mark any subset that contains a final state as a final DFA state
    next_partitions.into_iter().for_each(|subset| {
        // mark as final if it contains any of the original final states
        if !subset.is_disjoint(&final_states) {
            minimized_dfa_final_states.insert(next_state_no);
        }

        // map the subset of NFA states to the DFA state
        subset.into_iter().for_each(|state| {
            state_mapping.insert(state, next_state_no);
        });
        next_state_no += 1;
    });

    //create resulting automata
    let mut result_automata = DFA::new(next_state_no, minimized_dfa_final_states);

    // move transitions to new automata
    for label in &input.alphabet {
        for (source_state, target_state) in input.get_transitions(label) {
            let source_mapping = state_mapping.get(&source_state).unwrap();
            let target_mapping = state_mapping.get(&target_state).unwrap();
            result_automata.add_transition(*source_mapping, *target_mapping, label.clone());
        }
    }

    result_automata
}

/// helper function to check equivelance classes during DFA minimization
/// automata: the original DFA
/// partitions: a partitioning of DFA states, where each partition is a subset of the original DFA
/// state_1 & state_2: two states
/// returns true if two states are distinguishable based on the given partitions
fn is_distinguishable(automata: &DFA, partitions: &Vec<BTreeSet<u8>>, state_1: u8, state_2: u8) -> bool {
    let alphabet = &automata.alphabet;

    // check every label until finding a transition that proves these two states are distinguishable
    for label in alphabet {
        // obtain moves for both states
        let target_1 = automata.state_move(state_1, label);
        let target_2 = automata.state_move(state_2, label);

        if target_1.is_none() && target_2.is_none() {
            // both states have no transitions, so continue searching proofs
            continue;
        } else if target_1.is_some() && target_2.is_some() {
            // obtain target state in the original DFA
            let target_1 = target_1.unwrap();
            let target_2 = target_2.unwrap();

            // check if they both belong the same partition, if not return false
            for partition in partitions {
                if partition.contains(&target_1) != partition.contains(&target_2) {
                    // transitions do not lead to same state, so these are distinguishable
                    return true;
                }
            }
        } else {
            // one state has valid transition, the other has not so these are distinguishable
            return true;
        }
    }
    // no proof found, so states are indistinguishable
    return false;
}

/// helper function to copy states from one automata to other
/// offset is used to shift state numbers in the resulting automata
fn move_transitions(source: &NFA, target: &mut NFA, offset: u8) {
    // check target automata size is enough to hold all transitions
    assert!(source.num_states + offset <= target.num_states, format!("Transitions cannot be moved as target automata has only {} states, needed {}", target.num_states, source.num_states + offset));

    // carry over all labeled transitions
    for state in 0..source.num_states {
        source.get_outgoing_transitions(state)
            .iter().flat_map(move |(label, target_states)|
            target_states.iter().map(move |target_state| (label, *target_state))
        ).for_each(|(label, target_sate)|
            target.add_transition(state + offset, target_sate + offset, label.to_string())
        );
    }

    //carry over all epsilon transitions
    for state in 0..source.num_states {
        source.get_epsilon_transitions(state).iter()
            .for_each(|target_state| {
                target.add_epsilon_transition(state + offset, *target_state + offset)
            });
    }
}

/// unit-tests for Automata-related functionality
#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::iter::FromIterator;

    use crate::query::automata::{alternation, concatenation, determinize, kleene_plus, kleene_star, minimize, transition};
    use crate::query::automata::dfa::DFA;

    #[test]
    fn test_transition() {
        let label = transition("label".to_string());
        let target = label.state_move(0, "label");

        assert!(target.is_some());
        let targets = target.unwrap();
        assert_eq!(targets.len(), 1);
        assert!(label.final_states.contains(&targets[0]));
    }

    #[test]
    fn test_concat() {
        let a = transition("a".to_string());
        let b = transition("b".to_string());

        let concat = concatenation(a, b);

        assert_eq!(concat.final_states.len(), 1);
        assert_eq!(concat.get_epsilon_transitions(0).len(), 0);
        let move1 = concat.state_move(0, "a");
        assert!(move1.is_some());
        assert_eq!(move1.unwrap().len(), 1);
        assert!(concat.state_move(0, "b").is_none());
    }

    #[test]
    fn test_alternate() {
        let a = transition("a".to_string());
        let b = transition("b".to_string());

        let alternation = alternation(a, b);

        let move1 = alternation.state_move(1, "a");
        let move2 = alternation.state_move(3, "b");
        assert!(move1.is_some());
        assert!(move2.is_some());
        let e1 = alternation.get_epsilon_closure(move1.unwrap()[0]);
        let e2 = alternation.get_epsilon_closure(move2.unwrap()[0]);

        assert_eq!(e1.intersection(&e2).count(), 1)
    }

    #[test]
    fn test_kleene_star() {
        let a = transition("a".to_string());
        let b = transition("b".to_string());

        let kleene = kleene_star(concatenation(a, b));

        let move1 = kleene.state_move(1, "a");
        assert!(move1.is_some());

        let move2 = kleene.get_epsilon_transitions(4);
        assert!(move2.contains(&1));
        assert!(move2.contains(&5));

        let move3 = kleene.get_epsilon_transitions(0);
        assert!(move3.contains(&1));
        assert!(move3.contains(&(kleene.num_states - 1)));
    }

    #[test]
    fn test_kleene_plus() {
        let a = transition("a".to_string());
        let b = transition("b".to_string());

        let kleene = kleene_plus(concatenation(a, b));

        let move1 = kleene.state_move(1, "a");
        assert!(move1.is_some());

        let move2 = kleene.get_epsilon_transitions(4);
        assert!(move2.contains(&1));
        assert!(move2.contains(&5));


        let move3 = kleene.get_epsilon_transitions(0);
        assert!(move3.contains(&1));
        assert!(!move3.contains(&(kleene.num_states - 1)));
    }

    #[test]
    fn nfa_accept() {
        let a = transition("a".to_string());
        let b = transition("b".to_string());

        assert!(!a.accept(vec!["a", "a", "a"]));
        assert!(b.accept(vec!["b"]));
        assert!(!a.accept(vec!["a", "a"]));

        let kleene = kleene_star(concatenation(a.clone(), b.clone()));
        assert!(kleene.accept(vec![]));
        assert!(kleene.accept(vec!["a", "b"]));
        assert!(kleene.accept(vec!["a", "b", "a", "b"]));
        assert!(!kleene.accept(vec!["a"]));
        assert!(!kleene.accept(vec!["a", "a", "b"]));
        assert!(!kleene.accept(vec!["a", "b", "a"]));

        let alternation = alternation(a, b);
        assert!(alternation.accept(vec!["a"]));
        assert!(alternation.accept(vec!["b"]));
        assert!(!alternation.accept(vec!["a", "b"]));

        let kleene_alternation = kleene_star(alternation);
        assert!(kleene_alternation.accept(vec!["a"]));
        assert!(kleene_alternation.accept(vec!["b"]));
        assert!(kleene_alternation.accept(vec!["a", "b", "b", "a"]));
    }

    #[test]
    #[should_panic]
    fn nfa_panic() {
        let a = transition("a".to_string());
        assert!(a.accept(vec!["c"]));
    }

    #[test]
    fn dfa_accept() {
        let mut dfa = DFA::new(3, HashSet::<u8>::from_iter(vec![2].into_iter()));

        dfa.add_transition(0, 1, "a".to_string());
        dfa.add_transition(1, 2, "b".to_string());
        dfa.add_transition(2, 1, "a".to_string());

        assert!(!
            dfa.accept(vec![]));
        assert!(dfa.accept(vec!["a", "b"]));
        assert!(dfa.accept(vec!["a", "b", "a", "b"]));
        assert!(!dfa.accept(vec!["a"]));
        assert!(!dfa.accept(vec!["a", "a", "b"]));
        assert!(!dfa.accept(vec!["a", "b", "a"]));
    }

    #[test]
    #[should_panic]
    fn dfa_panic() {
        let mut dfa = DFA::new(3, HashSet::<u8>::from_iter(vec![2].into_iter()));

        dfa.add_transition(0, 1, "a".to_string());
        dfa.add_transition(1, 2, "b".to_string());
        dfa.add_transition(2, 1, "a".to_string());

        assert!(dfa.accept(vec!["a", "b", "a", "c"]));
    }

    #[test]
    fn test_determinize() {
        let a = transition("a".to_string());
        let b = transition("b".to_string());

        let kleene = determinize(kleene_star(concatenation(a.clone(), b.clone())));
        assert!(kleene.accept(vec![]));
        assert!(kleene.accept(vec!["a", "b"]));
        assert!(kleene.accept(vec!["a", "b", "a", "b"]));
        assert!(!kleene.accept(vec!["a"]));
        assert!(!kleene.accept(vec!["a", "a", "b"]));
        assert!(!kleene.accept(vec!["a", "b", "a"]));

        let kleene_alternation = determinize(kleene_star(alternation(a, b)));
        assert!(kleene_alternation.accept(vec!["a"]));
        assert!(kleene_alternation.accept(vec!["b"]));
        assert!(kleene_alternation.accept(vec!["a", "b", "b", "a"]));
    }

    #[test]
    fn test_minimize() {
        let a = transition("a".to_string());
        let b = transition("b".to_string());
        let kleene_alternation = determinize(kleene_star(alternation(a, b)));
        let dfa_states = kleene_alternation.num_states;

        assert!(kleene_alternation.accept(vec!["a"]));
        assert!(kleene_alternation.accept(vec!["b"]));
        assert!(kleene_alternation.accept(vec!["a", "b", "b", "a"]));


        let minimized = minimize(kleene_alternation);

        assert!(minimized.accept(vec!["a"]));
        assert!(minimized.accept(vec!["b"]));
        assert!(minimized.accept(vec!["a", "b", "b", "a"]));

        assert!(minimized.num_states <= dfa_states);
    }
}