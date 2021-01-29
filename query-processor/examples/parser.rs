use sgraffito_query::query::parser::RPQParser;

/// Simple RPQ-parser example
fn main() {
    let parser = RPQParser::new();
    let res = parser.parse_rpq("(knows/mentions/follows)+");
    match res {
        Ok(dfa) => {
            println!("Resulting DFA: {:?}", dfa);
        },
        Err(e) => {
            println!("Parse error: {}", e);
        }
    }
}