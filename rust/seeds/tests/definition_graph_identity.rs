//! Definition identity includes owned referenced structures, not their addresses
//! in the containing RX bundle. No Pill/PAC record layout is involved.
use data_seeds::{Document, Field, doc};
use pil_codec::doc::Type;

fn graph(width: u16) -> Document {
    doc::new("event")
        .field(Field::new("direct").object(1))
        .field(Field::new("array").array_object(1).size(2))
        .field(Field::new("map").map_object("tstr", 1))
        .field(Field::new("inline").record([Field::new("nested").object(1)]))
        .field(Field::new("choice").choice([Type::object(1), Type::new("bool")]))
        .definition("object")
        .field(Field::new("child").object(2))
        .definition("child")
        .field(Field::new("value").r#type("uint").size(width))
        .select(0).sign().unwrap()
}

#[test]
fn moving_a_complete_graph_preserves_each_member_digest() {
    let original=graph(2);
    let relocated=doc::new("unrelated")
        .field(Field::new("flag").r#type("bool"))
        .compound(original.bytes().unwrap()).sign().unwrap();
    let reopened=doc::read(relocated.bytes().unwrap()).unwrap();
    for old in original.definitions() {
        let new=reopened.definitions().iter().find(|d|d.header.rx_name==old.header.rx_name).unwrap();
        assert_eq!(old.header.rx_hash,new.header.rx_hash,"{}",old.header.rx_name);
    }
    assert_eq!(original.definitions()[0].fields[0].base_kind()[1],1);
    assert_eq!(reopened.definitions()[1].fields[0].base_kind()[1],2,"wire pointers must still be remapped");
}

#[test]
fn changing_referenced_content_changes_all_ancestor_digests() {
    let before=graph(2);
    let after=graph(3);
    for (a,b) in before.definitions().iter().zip(after.definitions()) {
        assert_ne!(a.header.rx_hash,b.header.rx_hash,"{} must include its referenced content",a.header.rx_name);
    }
}
