use data_seeds::ports::{
    Limits, Ports,
    format::{Cell, Profile},
    import::{Column, Mapping},
};
fn profile(kind: &str) -> Profile {
    Profile {
        kind: kind.into(),
        ..Default::default()
    }
}
fn sample(text: &str, p: Profile) -> data_seeds::ports::Candidate {
    Ports
        .sample_with(
            text.as_bytes(),
            Limits::default(),
            Some(text.len() as u64),
            Some(p),
        )
        .unwrap()
}
#[test]
fn xml_detection_prefers_event_structure_over_repeated_leaf_elements() {
    let text = b"<events><event at=\"2026-01-01T00:00:00Z\"><duration>25</duration><tag>a</tag><tag>b</tag></event></events>";
    let candidate = Ports
        .sample(text.as_slice(), Limits::default(), None)
        .unwrap();
    assert_eq!(candidate.profile.record, "/events/event");
    assert_eq!(candidate.records, 1);
    assert!(candidate.failures.is_empty());
    assert_eq!(candidate.preview[0].fields["tag"], "[a, b]");
}
#[test]
fn quoted_csv_is_not_split_on_lines_and_preserves_exact_values() {
    let text = "time,amount,note,status\r\n1700000000000,340282366920938463463374607431768211455,\"a,b\r\n\"\"quoted\"\"\",200\r\n";
    let p = profile("delimited");
    let s = sample(text, p.clone());
    assert!(s.failures.is_empty(), "{:?}", s.failures);
    assert_eq!(s.records, 1);
    assert_eq!(
        s.fields.iter().find(|f| f.name == "amount").unwrap().size,
        16
    );
    assert_eq!(s.preview[0].fields["note"], "a,b\r\n\"quoted\"");
    let plan = Mapping { log:false,
        format: p.clone(),
        name: "csv".into(),
        time_field: "time".into(),
        time_format: "unix_ms".into(),
        fields: vec![
            Column {
                name: "time".into(),
                kind: "uint".into(),
                size: 6,
                ..Default::default()
            },
            Column {
                name: "amount".into(),
                kind: "uint".into(),
                size: 16,
                ..Default::default()
            },
            Column {
                name: "note".into(),
                kind: "tstr".into(),
                size: 0,
                ..Default::default()
            },
            Column {
                name: "status".into(),
                kind: "uint".into(),
                size: 2,
                ..Default::default()
            },
        ],
    }
    .prepare()
    .unwrap();
    plan.document.validate().unwrap();
    assert!(
        plan.document.definitions()[0]
            .fields
            .iter()
            .any(|f| f.name == "amount" && f.size == 16)
    );
    let e = p
        .reader(text.as_bytes(), 1 << 20)
        .unwrap()
        .next()
        .unwrap()
        .unwrap();
    assert_eq!(plan.encode_event(&e, [0; 10]).unwrap().0, 1700000000000);
}
#[test]
fn delimiter_profiles_detect_and_validate_headers_nulls_and_width() {
    for delimiter in [',', '\t', ';', '|'] {
        let text = format!(
            "time{delimiter}value\n2026-01-01T00:00:00Z{delimiter}1\n2026-01-01T00:05:00Z{delimiter}2\n"
        );
        let s = Ports
            .sample(text.as_bytes(), Limits::default(), None)
            .unwrap();
        assert_eq!(s.profile.delimiter, delimiter.to_string());
        assert_eq!(s.records, 2);
    }
    let mut p = profile("delimited");
    p.header = false;
    p.columns = vec!["time".into(), "code".into(), "missing".into()];
    p.nulls = vec!["NA".into()];
    let s = sample("1700000000000,001,NA\n", p);
    assert_eq!(s.preview[0].fields["code"], "001");
    assert_eq!(
        s.fields.iter().find(|f| f.name == "code").unwrap().kind,
        "tstr"
    );
    assert_eq!(
        s.fields.iter().find(|f| f.name == "missing").unwrap().null,
        1
    );
    for bad in [
        "a,a\n1,2\n",
        "a,b\n1\n",
        "a,b\n1,\"unclosed\n",
        "a,b\n1,\"a\"junk\n",
    ] {
        assert!(
            !sample(bad, profile("delimited")).failures.is_empty(),
            "{bad}"
        );
    }
}
#[test]
fn fixed_columns_preserve_utf8_and_do_not_silently_cut_characters() {
    let mut p = profile("fixed");
    p.boundaries = vec![13, 17];
    p.columns = vec!["time".into(), "code".into(), "message".into()];
    p.trim = true;
    let s = sample("1700000000000ERR héllo\n", p.clone());
    assert!(s.failures.is_empty());
    assert_eq!(s.preview[0].fields["code"], "ERR");
    p.boundaries = vec![1];
    p.columns = vec![];
    assert!(!sample("éx\n", p).failures.is_empty());
}
#[test]
fn multiline_patterns_and_logfmt_are_bounded_and_named() {
    let mut p = profile("pattern");
    p.pattern = r"^(?P<time>\S+) (?P<level>\S+) (?P<message>[\s\S]*)$".into();
    p.start = r"^\d{4}-".into();
    let s = sample(
        "2026-01-01T00:00:00Z ERROR broken\n  at example:12\n2026-01-01T00:01:00Z INFO ok\n",
        p.clone(),
    );
    assert_eq!(s.records, 2);
    assert!(s.preview[0].fields["message"].contains("at example"));
    assert_eq!(s.time[0].field, "time");
    assert!(
        p.reader("orphan\n".as_bytes(), 100)
            .unwrap()
            .next()
            .is_err()
    );
    let s = sample(
        "time=1700000000000 code=200 message=\"two words\"\n",
        profile("logfmt"),
    );
    assert_eq!(s.preview[0].fields["message"], "two words");
    assert!(!sample("a=1 a=2\n", profile("logfmt")).failures.is_empty());
    assert!(
        p.reader("2026-01-01T00:00:00Z ERROR huge\n".as_bytes(), 8)
            .unwrap()
            .next()
            .is_err()
    );
}
#[test]
fn xml_event_path_attributes_entities_and_repeated_children() {
    let mut p = profile("xml");
    p.record = "/events/event".into();
    let text = "<events><event time=\"2026-01-01T00:00:00Z\"><value>42</value><tag>A &amp; B</tag><tag><![CDATA[<C>]]></tag></event><event time=\"2026-01-01T00:05:00Z\"><value>43</value></event></events>";
    let s = sample(text, p.clone());
    assert_eq!(s.records, 2, "{:?}", s.failures);
    assert_eq!(s.time[0].field, "@time");
    assert_eq!(s.preview[0].fields["tag"], "[A & B, <C>]");
    assert!(s.preview[0].raw.starts_with("<event "));
    let mut r = p.reader(text.as_bytes(), 1 << 20).unwrap();
    assert!(matches!(
        r.next().unwrap().unwrap().fields["tag"],
        Cell::List(_)
    ));
    for bad in [
        "<!DOCTYPE events SYSTEM 'file:///etc/passwd'><events/>",
        "<events><event><v>1</bad></event></events>",
        "<events><event><v>&unknown;</v></event></events>",
    ] {
        assert!(!sample(bad, p.clone()).failures.is_empty(), "{bad}")
    }
    assert!(p.reader(text.as_bytes(), 20).unwrap().next().is_err());
}
#[test]
fn syslog_modern_and_legacy_do_not_invent_time() {
    let p = profile("syslog");
    let s = sample(
        "<34>1 2026-01-01T00:00:00Z host app 42 msg [example@32473 key=\"a\\]b\"] hello\n",
        p.clone(),
    );
    assert_eq!(s.records, 1, "{:?}", s.failures);
    assert_eq!(s.preview[0].fields["severity"], "2");
    assert_eq!(s.preview[0].fields["data.example@32473.key"], "a]b");
    let text = "<34>Oct 11 22:14:15 host app: hello\n";
    assert!(!sample(text, p.clone()).failures.is_empty());
    let mut p = p;
    p.year = Some(2026);
    p.offset = Some("+00:00".into());
    let s = sample(text, p);
    assert_eq!(s.preview[0].fields["time"], "2026-10-11T22:14:15+00:00");
}
#[test]
fn incomplete_sample_never_advertises_a_partial_event() {
    let text = "a,b\n1,ok\n2,\"half";
    let s = Ports
        .sample_with(
            text.as_bytes(),
            Limits::default(),
            Some(5000),
            Some(profile("delimited")),
        )
        .unwrap();
    assert_eq!(s.records, 1);
    assert!(!s.complete);
    assert!(s.failures.is_empty());
}
