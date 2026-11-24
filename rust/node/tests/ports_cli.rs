use std::io::Cursor;

fn run(args: &[&str], input: &[u8]) -> Vec<u8> {
    let mut output = Vec::new();
    trunk_node::command::run(
        &args.iter().map(|s| s.to_string()).collect::<Vec<_>>(),
        Cursor::new(input),
        &mut output,
        &mut Vec::new(),
        false,
    )
    .unwrap();
    output
}

#[test]
fn stdin_format_inspection_and_doc_review_need_no_node() {
    let output = run(
        &["ports", "inspect", "-"],
        b"time,duration\n1700000000000,25\n",
    );
    let candidate: serde_json::Value = serde_json::from_slice(&output).unwrap();
    assert_eq!(candidate["profile"]["kind"], "delimited");
    assert_eq!(candidate["records"], 1);
    let mapping = br##"{"name":"sample","time_field":"time","time_format":"unix_ms","format":{"kind":"delimited"},"fields":[{"name":"time","kind":"uint","size":6},{"name":"duration","kind":"uint","size":1,"annotations":["@unit(ms)","#summ(numeric)"]}]}"##;
    let reviewed = run(&["ports", "review", "-"], mapping);
    let review: serde_json::Value = serde_json::from_slice(&reviewed).unwrap();
    assert!(review.to_string().contains("duration"));
    let rx = run(&["ports", "review", "-", "--rx"], mapping);
    let expected: data_seeds::ports::import::Mapping = serde_json::from_slice(mapping).unwrap();
    assert_eq!(rx, expected.prepare().unwrap().document.bytes().unwrap());
}
