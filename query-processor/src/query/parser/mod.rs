use log::trace;
use pest::iterators::Pair;
use pest::Parser;
use pest_derive::Parser;

use crate::query::{automata::dfa::DFA, automata::nfa::NFA};
use crate::query::automata::{alternation, concatenation, determinize, kleene_plus, kleene_star, minimize, transition};

/// PEST based parser for Regular Path Queries
/// It uses a subset of the SPARQL property path syntax to express RPQ, grammar is at `rpq.pest`

#[derive(Parser)]
#[grammar = "query/parser/rpq.pest"]
pub struct RPQParser;

impl RPQParser {
    pub fn new() -> Self {
        Self {}
    }

    pub fn parse_rpq(&self, query_str: &str) -> Result<DFA, String> {
        let parse_result = RPQParser::parse(Rule::RPQ, query_str).expect("RPQ Parser unsuccessfull").next().unwrap();

        let mut results = Vec::new();

        for pair in parse_result.into_inner() {
            let result = match pair.as_rule() {
                Rule::Path => {
                    let res = self.parse_path(pair);
                    res.map(|nfa| minimize(determinize(nfa)))
                }
                r => {
                    trace!("{:?}", pair);
                    Err(format!("Rule {:?} is not recognized", r))
                }
            };
            results.push(result)
        }

        results.remove(0)
    }

    fn parse_primary(&self, pair: Pair<Rule>) -> Result<NFA, String> {
        trace!("PathPrimary: {:?}", pair);
        if let Some(primary) = pair.into_inner().next() {
            match primary.as_rule() {
                Rule::predicate => {
                    let label = primary.as_str().to_string();
                    Ok(transition(label))
                }
                Rule::Path => {
                    self.parse_path(primary)
                }
                _ => {
                    Err(format!("PathPrimary can consist of only Path or predicate {}", primary.as_str()).to_string())
                }
            }
        } else {
            Err(format!("PathPrimary is a Path or a predicate").to_string())
        }
    }

    fn parse_elt(&self, pair: Pair<Rule>) -> Result<NFA, String> {
        trace!("PathElt: {:?}", pair);
        if let Some(path_elt) = pair.into_inner().next() {
            match path_elt.as_rule() {
                Rule::PathElt => {
                    let mut path_elt_iterator = path_elt.into_inner();

                    if let Some(path_primary) = path_elt_iterator.next() {
                        let primary = self.parse_primary(path_primary)?;

                        if let Some(path_mod) = path_elt_iterator.next() {
                            match path_mod.as_str() {
                                "*" => {
                                    Ok(kleene_star(primary))
                                }
                                "+" => {
                                    Ok(kleene_plus(primary))
                                }
                                _ => {
                                    Err("Bounded RPQ is not supported".to_string())
                                }
                            }
                        } else {
                            Ok(primary)
                        }
                    } else {
                        Err("PathElt should include at least one path primary".to_string())
                    }
                }
                _ => {
                    Err(format!("PathElt consist of PathAlternative {}", path_elt.as_str()).to_string())
                }
            }
        } else {
            Err("PathEltOrInverse should consist of PathElt".to_string())
        }
    }

    fn parse_sequence(&self, pair: Pair<Rule>) -> Result<NFA, String> {
        trace!("PathSequence: {:?}", pair);
        // obtain the first NFA, then use concat
        let mut sequence_iterator = pair.into_inner();

        if let Some(first_seq) = sequence_iterator.next() {
            let mut first = self.parse_elt(first_seq)?;

            while let Some(seq) = sequence_iterator.next() {
                let seq_nfa = self.parse_elt(seq)?;
                first = concatenation(first, seq_nfa)
            }

            Ok(first)
        } else {
            Err("Concatenation should have at least one element".to_string())
        }
    }

    fn parse_alternative(&self, pair: Pair<Rule>) -> Result<NFA, String> {
        trace!("PathAlternative: {:?}", pair);
        // obtain the first NFA, then use alternation
        let mut alternation_iterator = pair.into_inner();

        if let Some(first_alternation) = alternation_iterator.next() {
            let mut first = self.parse_sequence(first_alternation)?;

            while let Some(alt) = alternation_iterator.next() {
                let alt_nfa = self.parse_sequence(alt)?;
                first = alternation(first, alt_nfa)
            }

            Ok(first)
        } else {
            Err("Alternation should have at least one element".to_string())
        }
    }

    fn parse_path(&self, pair: Pair<Rule>) -> Result<NFA, String> {
        trace!("Path: {:?}", pair);

        if let Some(alternative_rule) = pair.into_inner().next() {
            match alternative_rule.as_rule() {
                Rule::PathAlternative => {
                    self.parse_alternative(alternative_rule)
                }
                _ => {
                    Err(format!("Path should consist of PathAlternative {}", alternative_rule.as_str()).to_string())
                }
            }
        } else {
            Err("Path should consist of PathAlternative".to_string())
        }
    }
}