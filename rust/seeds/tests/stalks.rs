use data_seeds::{*,stalks::{State,Values}};
use ciborium::Value as Data;
use std::sync::Arc;
fn definition()->Arc<SeedDefinition> {
    SeedDefinition::prescribe(&doc::new("stalk-test")
        .field(Field::new("n").r#type("uint").size(4))
        .field(Field::new("wide").r#type("uint").size(16))
        .field(Field::new("note").r#type("tstr"))
        .field(Field::new("active").r#type("bool"))
        .sign().unwrap()).unwrap()
}
fn pack(def:&Arc<SeedDefinition>,start:u32,count:u32)->Arc<SeedPackSnapshot> {
    let mut pack=def.fill(count as usize).unwrap();
    for n in start..start+count {
        let note=format!("event-{n}");
        pack.load(def.ingest(n,|n,s|{
            s.set_uint("n",n)?.set_uint("wide",(1u128<<100)+n as u128)?.set_bool("active",n%2==0)?;
            if n%3==0 {s.set_null("note")?;} else if n%3==1 {s.set_text_string("note",&note)?;}
            Ok(())
        }).unwrap()).unwrap();
    }
    pack.seal().unwrap();def.open(pack.into_bytes().unwrap()).unwrap()
}
#[test]
fn beds_grow_into_owned_columns_and_release_each_consumed_pack() {
    let def=definition();let a=pack(&def,0,300);let b=pack(&def,300,300);let wa=Arc::downgrade(&a);let wb=Arc::downgrade(&b);
    let mut beds=Beds::new();beds.pack(a).pack(b);let budget=MemoryBudget::new(1_000_000);
    let mut growth=beds.stalks(Query::new().select(["n","wide","note","active"]),budget.clone(),1000).unwrap();
    let mut released_early=false;
    loop {let done=growth.poll(128).unwrap();if wa.upgrade().is_none()&&!done {released_early=true;}if done {break;}}
    assert!(released_early);assert!(wa.upgrade().is_none());assert!(wb.upgrade().is_none());
    let stalks=growth.finish().unwrap();assert_eq!(stalks.len(),600);
    assert_eq!(stalks.sum("n").unwrap(),Cell::Value(Data::Integer((599u64*600/2).into())));
    assert_eq!(stalks.sum("wide").unwrap(),Cell::Value(pil_codec::native_unsigned((1u128<<100)*600+599*600/2)));
    assert!(stalks.object(0).unwrap().iter().any(|(n,v)|n=="note"&&*v==Data::Null));
    assert!(stalks.object(1).unwrap().iter().any(|(n,v)|n=="note"&&*v==Data::Text("event-1".into())));
    assert!(!stalks.object(2).unwrap().iter().any(|(n,_)|n=="note"));
    assert!(matches!(stalks.chunks()[0].column("n").unwrap().values(),Values::Unsigned(_)));
    assert_eq!(stalks.chunks()[0].column("note").unwrap().state(2).unwrap(),State::Absent);
    assert!(budget.used()>0);drop(stalks);assert_eq!(budget.used(),0);
}
#[test]
fn global_filter_limit_and_order_remain_the_existing_querys_job() {
    let def=definition();let mut beds=Beds::new();beds.pack(pack(&def,0,10)).pack(pack(&def,10,10));
    let budget=MemoryBudget::new(100_000);
    let mut growth=beds.stalks(Query::new().select(["n"]).filter(Predicate::compare("n",Compare::Ge,8)).order_by("n",Direction::Descending).limit(3),budget.clone(),3).unwrap();
    while !growth.poll(4).unwrap() {}
    let stalks=growth.finish().unwrap();assert_eq!(stalks.len(),3);assert_eq!(stalks.sum("n").unwrap(),Cell::Value(54u64.into()));
    assert_eq!(stalks.object(0).unwrap(),vec![("n".into(),19u64.into())]);drop(stalks);assert_eq!(budget.used(),0);
}
#[test]
fn cancellation_and_failed_admission_release_partial_columns_and_packs() {
    for limit in [0,3000,1_000_000] {
        let def=definition();let pack=pack(&def,0,600);let weak=Arc::downgrade(&pack);let mut beds=Beds::new();beds.pack(pack);
        let budget=MemoryBudget::new(limit);let mut growth=beds.stalks(Query::new(),budget.clone(),1000).unwrap();
        if limit<1_000_000 {assert!(growth.poll(256).is_err());}else{assert!(!growth.poll(100).unwrap());assert!(budget.used()>0);growth.cancel();}
        assert_eq!(budget.used(),0);assert!(weak.upgrade().is_none());assert!(growth.finish().is_err());
    }
}
#[test]
fn enum_columns_preserve_labels_and_signed_numeric_categories() {
    let doc=doc::new("stalk-enums")
        .field(Field::new("status").enumeration("uint",[200u32.into(),500u32.into()]))
        .field(Field::new("label").enumeration("tstr",["ok".into(),"error".into()]))
        .field(Field::new("signed").enumeration("int",[1i64.into(),(-1i64).into()])).sign().unwrap();
    let def=SeedDefinition::prescribe(&doc).unwrap();let mut pack=def.fill(2).unwrap();
    for (status,label,signed) in [(200u32,"ok",1i64),(500,"error",-1)] {
        pack.load(def.ingest((),|(),s|{s.set_enum("status",status.into())?.set_enum("label",label.into())?.set_enum("signed",signed.into())?;Ok(())}).unwrap()).unwrap();
    }
    pack.seal().unwrap();let mut beds=Beds::new();beds.pack(def.open(pack.into_bytes().unwrap()).unwrap());
    let mut g=beds.stalks(Query::new(),MemoryBudget::new(100_000),2).unwrap();while !g.poll(10).unwrap(){}let stalk=g.finish().unwrap();
    assert_eq!(stalk.sum("status").unwrap(),Cell::Value(700u64.into()));assert_eq!(stalk.sum("signed").unwrap(),Cell::Value(0i64.into()));
    assert!(stalk.object(1).unwrap().iter().any(|(n,v)|n=="label"&&*v==Data::Text("error".into())));
}
#[test]
fn growth_respects_other_readers_and_never_truncates_to_the_row_budget() {
    let def=definition();let retained=pack(&def,0,10);let mut beds=Beds::new();beds.pack(retained.clone());
    let budget=MemoryBudget::new(100_000);let mut growth=beds.stalks(Query::new(),budget.clone(),9).unwrap();
    assert!(growth.poll(100).is_err());assert_eq!(budget.used(),0);assert_eq!(Arc::strong_count(&retained),1);
}
#[test]
fn differing_definitions_keep_typed_chunks_without_rounding_wide_values() {
    let mut beds=Beds::new();
    for (width,n) in [(4,7u128),(16,1u128<<100)] {
        let doc=doc::new(format!("shape-{width}")).field(Field::new("n").r#type("uint").size(width)).sign().unwrap();
        let def=SeedDefinition::prescribe(&doc).unwrap();let mut pack=def.fill(1).unwrap();
        pack.load(def.ingest(n,|n,s|{s.set_uint("n",n)?;Ok(())}).unwrap()).unwrap();pack.seal().unwrap();
        beds.pack(def.open(pack.into_bytes().unwrap()).unwrap());
    }
    let budget=MemoryBudget::new(10_000);let mut g=beds.stalks(Query::new().select(["n"]),budget.clone(),2).unwrap();
    while !g.poll(10).unwrap() {}let stalk=g.finish().unwrap();
    assert_eq!(stalk.chunks().len(),2);assert_eq!(stalk.sum("n").unwrap(),Cell::Value(pil_codec::native_unsigned((1u128<<100)+7)));
    drop(stalk);assert_eq!(budget.used(),0);
}
