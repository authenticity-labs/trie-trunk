//! Editing assistance uses the SQL parser's lexer, including quoted names and comments.
//! Suggestions do not alter execution support or bypass query validation.
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sqlparser::{
    dialect::AnsiDialect,
    tokenizer::{Location, Token, TokenWithSpan, Tokenizer, Whitespace},
};
#[derive(Clone, Deserialize)]
pub struct Field {
    pub name: String,
    #[serde(rename = "type")]
    pub kind: String,
    #[serde(default)]
    pub types: Vec<String>,
    #[serde(default)]
    pub values: Vec<String>,
    #[serde(default)]
    pub detail: Option<String>,
    #[serde(default)]
    pub metadata: Option<crate::metadata::Profile>,
}
#[derive(Serialize)]
struct Item {
    label: String,
    detail: String,
    insert: String,
    from: usize,
    to: usize,
    cursor: Option<usize>,
    ready: bool,
}
fn byte_at(text: &str, loc: Location) -> usize {
    let (mut line, mut column) = (1, 1);
    for (i, c) in text.char_indices() {
        if line == loc.line && column == loc.column {
            return i;
        }
        if c == '\n' {
            line += 1;
            column = 1
        } else {
            column += 1
        }
    }
    text.len()
}
fn at_utf16(text: &str, n: usize) -> usize {
    let mut at = 0;
    for (i, c) in text.char_indices() {
        if at >= n {
            return i;
        }
        at += c.len_utf16()
    }
    text.len()
}
fn units(text: &str, byte: usize) -> usize {
    text[..byte].encode_utf16().count()
}
fn word(t: &Token) -> Option<&str> {
    if let Token::Word(w) = t {
        Some(&w.value)
    } else {
        None
    }
}
fn keyword(t: &Token, k: &str) -> bool {
    matches!(t,Token::Word(w) if w.quote_style.is_none()&&w.value.eq_ignore_ascii_case(k))
}
fn lex(text: &str) -> (Vec<TokenWithSpan>, bool) {
    let mut t = vec![];
    let ok = Tokenizer::new(&AnsiDialect {}, text)
        .tokenize_with_location_into_buf(&mut t)
        .is_ok();
    (t, ok)
}
fn numeric(f: &Field) -> bool {
    fn scalar(kind: &str) -> bool {
        kind.starts_with("TIMESTAMP_") || kind == "REF16"
            || ['U', 'I', 'F'].iter().any(|p| kind.strip_prefix(*p)
                .is_some_and(|n| !n.is_empty() && n.chars().all(|c| c.is_ascii_digit())))
            || ["UINT(", "INT("].iter().any(|p| kind.strip_prefix(p)
                .and_then(|n| n.strip_suffix(')'))
                .is_some_and(|n| !n.is_empty() && n.chars().all(|c| c.is_ascii_digit())))
    }
    if f.types.is_empty() { scalar(&f.kind) } else { f.types.iter().all(|kind| scalar(kind)) }
}

fn identifier(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}
fn string(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}
struct Menu {
    items: Vec<Item>,
    partial: String,
    from: usize,
    to: usize,
}
impl Menu {
    fn metadata(&mut self, field: &Field, quoted: bool) {
        use crate::metadata::Value;
        let Some(profile) = &field.metadata else { return };
        for group in &profile.groups {
            self.add(&group.label, &format!("Category · {}–{}", group.min, group.max),
                format!("{}{}", string(&group.label), if quoted { "" } else { " " }), None);
        }
        for mapping in &profile.values {
            let (value, insert) = match &mapping.value {
                Value::Integer(value) if !quoted => (value.to_string(), value.to_string()),
                Value::Text(value) => (value.clone(), string(value)),
                _ => continue,
            };
            let label = if value == mapping.label { value.clone() }
                else { format!("{value} — {}", mapping.label) };
            // Both canonical values and their human labels are searchable.
            let partial = self.partial.clone();
            if mapping.label.to_lowercase().starts_with(&partial.to_lowercase()) {
                self.partial.clear();
            }
            self.add(&label, "Metadata value", if quoted { insert } else { format!("{insert} ") }, None);
            self.partial = partial;
        }
    }
    fn add(&mut self, label: &str, detail: &str, insert: String, cursor: Option<usize>) {
        if !label
            .to_lowercase()
            .starts_with(&self.partial.to_lowercase())
        {
            return;
        }
        if !self.items.iter().any(|i| i.label == label) {
            self.items.push(Item {
                label: label.into(),
                detail: detail.into(),
                insert,
                from: self.from,
                to: self.to,
                cursor,
                ready: true,
            })
        }
    }
    fn keyword(&mut self, label: &str) {
        self.add(label, "SQL Clause", format!("{label} "), None)
    }
    fn fields(&mut self, fields: &[Field], numeric_only: bool) {
        for f in fields {
            if !numeric_only || numeric(f) {
                self.add(&f.name, f.detail.as_deref().unwrap_or(&f.kind), format!("{} ", identifier(&f.name)), None)
            }
        }
    }
}
pub fn complete(text: &str, caret: usize, fields: &[Field]) -> Result<Value, String> {
    if text.len() > 8192 {
        return Ok(json!({"items":[]}));
    }
    let end = at_utf16(text, caret);
    let prefix = &text[..end];
    let (all, _) = lex(text);
    let (mut tokens, ok) = lex(prefix);
    // Comments intentionally offer no completions.
    if let Some(t) = tokens.last() {
        if matches!(
            &t.token,
            Token::Whitespace(Whitespace::SingleLineComment { .. })
        ) && byte_at(prefix, t.span.end) == end
            && !prefix.ends_with('\n')
        {
            return Ok(json!({"items":[]}));
        }
    }
    let tail = tokens
        .last()
        .map(|t| byte_at(prefix, t.span.end))
        .unwrap_or(0);
    let rest = &prefix[tail..];
    let mut quoted = None;
    let mut partial = String::new();
    let mut from = end;
    let mut to = end;
    if !ok {
        let start = tail + rest.len() - rest.trim_start().len();
        let raw = &prefix[start..];
        if raw.starts_with('\'') || raw.starts_with('"') {
            let q = raw.chars().next().unwrap();
            quoted = Some(q);
            from = start;
            partial = raw[q.len_utf8()..].replace(&format!("{q}{q}"), &q.to_string());
        } else {
            return Ok(json!({"items":[]}));
        }
    } else if let Some(t) = tokens.last() {
        if byte_at(prefix, t.span.end) == end {
            if let Token::Word(w) = &t.token {
                from = byte_at(prefix, t.span.start);
                partial = w.value.clone();
                tokens.pop();
            }
        }
    }
    if from < end {
        to = all
            .iter()
            .find(|t| byte_at(text, t.span.start) == from)
            .map(|t| byte_at(text, t.span.end))
            .unwrap_or(end);
    }
    // Determine the source from non-whitespace tokens, even after the caret.
    let full: Vec<_> = all
        .iter()
        .filter(|t| !matches!(t.token, Token::Whitespace(_)))
        .collect();
    if full.windows(2).any(|w| keyword(&w[0].token, "FROM") && keyword(&w[1].token, "summaries")) {
        return Ok(json!({"items":[]}));
    }
    let t: Vec<_> = tokens
        .iter()
        .filter(|t| !matches!(t.token, Token::Whitespace(_)))
        .map(|t| &t.token)
        .collect();
    let last = t.last().copied();
    let previous = t.iter().rev().nth(1).copied();
    let value_field = if last.is_some_and(|t| {
        matches!(
            t,
            Token::Eq | Token::Neq | Token::Gt | Token::GtEq | Token::Lt | Token::LtEq
        )
    }) {
        previous
            .and_then(word)
            .and_then(|n| fields.iter().find(|f| f.name == n))
    } else {
        None
    };
    let mut menu = Menu {
        items: vec![],
        partial,
        from: units(text, from),
        to: units(text, to),
    };
    if quoted == Some('\'') {
        let Some(f) = value_field else {
            return Ok(json!({"items":[]}));
        };
        menu.metadata(f, true);
        for v in &f.values {
            menu.add(v, "Definition value", string(v), None)
        }
        let typed = menu.partial.clone();
        let word = typed
            .chars()
            .rev()
            .take_while(|c| c.is_ascii_alphabetic())
            .collect::<String>()
            .chars()
            .rev()
            .collect::<String>();
        let word_end = end
            + text[end..]
                .chars()
                .take_while(|c| c.is_ascii_alphabetic())
                .map(char::len_utf8)
                .sum::<usize>();
        let context = json!({"quote":menu.from,"end":menu.to,"typed":typed,"field":f.name,"word":if word.len()>=2{Some(&word)}else{None},"wordStart":caret-word.len(),"wordEnd":units(text,word_end)});
        return Ok(
            json!({"items":menu.items.into_iter().take(30).collect::<Vec<_>>(),"text":context}),
        );
    }
    if let Some(f) = value_field {
        menu.metadata(f, false);
        for v in &f.values {
            menu.add(v, "Mapped value", format!("{} ", string(v)), None)
        }
        if f.kind == "BOOL" {
            menu.add("TRUE", "Boolean", "TRUE ".into(), None);
            menu.add("FALSE", "Boolean", "FALSE ".into(), None)
        } else if numeric(f) {
            menu.add("0", "Numeric value", "0 ".into(), None)
        } else if f.values.is_empty() {
            menu.add("Enter text…", "Text value", "''".into(), Some(1))
        }
        return Ok(json!({"items":menu.items.into_iter().take(30).collect::<Vec<_>>()}));
    }
    // Recognize the active top-level clause without treating keywords in strings or names as syntax.
    let (mut depth, mut clause) = (0i32, "");
    let mut aggregate = false;
    for (i, token) in t.iter().enumerate() {
        if matches!(token, Token::LParen) {
            depth += 1
        }
        if matches!(token, Token::RParen) {
            depth -= 1
        }
        if ["COUNT", "SUM", "MIN", "MAX", "AVG"]
            .iter()
            .any(|k| keyword(token, k))
            && t.get(i + 1).is_some_and(|v| matches!(v, Token::LParen))
        {
            aggregate = true
        }
        if depth == 0 {
            for k in ["SELECT", "FROM", "WHERE", "GROUP", "ORDER"] {
                if keyword(token, k) {
                    clause = k
                }
            }
        }
    }
    if let Some(open)=t.iter().rposition(|t|matches!(t,Token::LParen)) {
        if open>0 && ["CAST","TRY_CAST"].iter().any(|k|keyword(&t[open-1],k)) && !t[open+1..].iter().any(|t|matches!(t,Token::RParen)) {
            if t.len()==open+1 {menu.fields(fields,false);}
            else if t[open+1..].iter().any(|t|keyword(t,"AS")) {
                for ty in ["SMALLINT","INTEGER","BIGINT","REAL","DOUBLE PRECISION","BOOLEAN"] {menu.add(ty,"Cast Type",format!("{ty}) "),None);}
            }else{menu.keyword("AS");}
            return Ok(json!({"items":menu.items}));
        }
    }
    let is = |k: &str| last.is_some_and(|t| keyword(t, k));
    let follow = |m: &mut Menu| {
        m.keyword("AND");
        m.keyword("OR");
        if aggregate {
            m.keyword("GROUP BY")
        } else {
            m.keyword("ORDER BY")
        }
    };
    if t.is_empty() {
        menu.keyword("SELECT")
    } else if last.is_some_and(|t| matches!(t, Token::LParen))
        && previous.is_some_and(|t| {
            ["COUNT", "SUM", "MIN", "MAX", "AVG"]
                .iter()
                .any(|k| keyword(t, k))
        })
    {
        if previous.is_some_and(|t| keyword(t, "COUNT")) {
            menu.add("*", "All Rows", "*) ".into(), None)
        }
        let numeric_only = !previous.is_some_and(|t| keyword(t, "COUNT"));
        for f in fields {
            if !numeric_only || numeric(f) {
                menu.add(&f.name, f.detail.as_deref().unwrap_or(&f.kind), format!("{}) ", identifier(&f.name)), None)
            }
        }
    } else {
        match clause {
            "SELECT" => {
                if is("SELECT") || last.is_some_and(|t| matches!(t, Token::Comma)) {
                    if is("SELECT") {
                        menu.add("*", "All Fields", "* ".into(), None)
                    }
                    menu.fields(fields, false);
                    for name in ["CAST","TRY_CAST"] {menu.add(name,"Conversion",format!("{name}("),None);}
                    for name in ["COUNT", "SUM", "MIN", "MAX", "AVG"] {
                        menu.add(name, "Aggregate", format!("{name}("), None)
                    }
                } else if !is("AS") {
                    menu.keyword("FROM");
                    menu.keyword("AS");
                    menu.add(",", "Next Field", ", ".into(), None)
                }
            }
            "FROM" => {
                if is("FROM") {
                    menu.add("events", "Event Source", "events ".into(), None)
                } else {
                    menu.keyword("WHERE");
                    if aggregate {
                        menu.keyword("GROUP BY")
                    } else {
                        menu.keyword("ORDER BY")
                    }
                }
            }
            "WHERE" => {
                if is("WHERE")
                    || is("AND")
                    || is("OR")
                    || is("NOT") && !previous.is_some_and(|t| keyword(t, "IS"))
                    || last.is_some_and(|t| matches!(t, Token::LParen))
                {
                    menu.fields(fields, false);
                    for name in ["CAST","TRY_CAST"] {menu.add(name,"Conversion",format!("{name}("),None);}
                    menu.keyword("NOT")
                } else if last.is_some_and(|t|matches!(t,Token::RParen)) && t.iter().any(|t|keyword(t,"CAST")||keyword(t,"TRY_CAST")) {
                    for op in ["=","<>",">",">=","<","<=","IS NULL","IS NOT NULL"]{menu.add(op,"Comparison",format!("{op} "),None);}
                } else if is("IS") {
                    menu.keyword("NULL");
                    menu.keyword("NOT NULL")
                } else if is("NOT") {
                    menu.keyword("NULL")
                } else if let Some(f) = last
                    .and_then(word)
                    .and_then(|n| fields.iter().find(|f| f.name == n))
                {
                    let ops = if f.kind == "BOOL" || f.kind == "UUID" {
                        &["=", "<>"][..]
                    } else {
                        &["=", "<>", ">", ">=", "<", "<="][..]
                    };
                    for op in ops {
                        menu.add(op, "Comparison", format!("{op} "), None)
                    }
                    menu.keyword("IS NULL");
                    menu.keyword("IS NOT NULL");
                } else {
                    follow(&mut menu)
                }
            }
            "GROUP" => {
                if is("GROUP") {
                    menu.keyword("BY")
                } else if is("BY") || last.is_some_and(|t| matches!(t, Token::Comma)) {
                    menu.fields(fields, false)
                } else {
                    menu.add(",", "Next Group", ", ".into(), None)
                }
            }
            "ORDER" => {
                if is("ORDER") {
                    menu.keyword("BY")
                } else if is("BY") {
                    menu.add("timestamp", "Time Ordering", "timestamp ".into(), None)
                } else if is("timestamp") {
                    menu.keyword("ASC");
                    menu.keyword("DESC")
                }
            }
            _ => {}
        }
    }
    Ok(json!({"items":menu.items.into_iter().take(30).collect::<Vec<_>>()}))
}
