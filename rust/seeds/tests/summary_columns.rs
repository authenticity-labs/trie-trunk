use data_seeds::{doc, Field, SeedDefinition, MemoryBudget, Predicate, summaries::*};
fn reader() -> SummaryReader {
    let doc=doc::new("column-test").field(Field::new("duration").r#type("uint").size(4).annotation("@unit(us)").annotation("#summ(numeric)")).sign().unwrap();
    let source=SeedDefinition::prescribe(&doc).unwrap();let plan=source.summaries(288).unwrap();let mut day=plan.accumulator(0).unwrap();
    for (time,value) in [(0,10u32),(300_000,30)] {day.ingest(time,value,|v,s|{s.set_uint("duration",v)?;Ok(())}).unwrap();}
    day.complete(day.end()).unwrap();let mut storage=SummaryBox::new(plan.layout().clone()).unwrap();storage.append(day.seal().unwrap()).unwrap().commit().unwrap();
    SummaryReader::open(storage.bytes().unwrap().to_vec(),0).unwrap()
}
#[test]
fn column_report_reuses_values_after_reader_release_and_releases_budget() {
    let budget=MemoryBudget::new(1_000_000);let mut source=reader();
    let mut report=SummaryReport::new(0,86_400_000,3_600_000,"duration").unwrap().memory_budget(budget.clone()).unwrap();
    // Reverse delivery preserves source/time ordering.
    report.consume(&mut source,"demo/service/resource",300_000,86_400_000,&Predicate::All).unwrap();
    report.consume(&mut source,"demo/service/resource",0,300_000,&Predicate::All).unwrap();
    assert!(report.consume(&mut source,"demo/service/resource",0,300_000,&Predicate::All).is_err());
    drop(source);let result=report.finish().unwrap();
    assert_eq!(result["dashboard"]["totals"]["events"],2);assert_eq!(result["dashboard"]["totals"]["average"],20.0);
    assert_eq!(report.finish().unwrap(),result);assert!(budget.used()>0);drop(report);assert_eq!(budget.used(),0);
}
#[test]
fn column_admission_failure_releases_partial_columns() {
    let budget=MemoryBudget::new(3000);let mut source=reader();
    let mut report=SummaryReport::new(0,86_400_000,3_600_000,"duration").unwrap().memory_budget(budget.clone()).unwrap();
    assert!(report.consume(&mut source,"demo/service/resource",0,86_400_000,&Predicate::All).is_err());
    assert_eq!(budget.used(),0);assert_eq!(report.finish().unwrap()["dashboard"]["totals"]["events"],0);
}

#[cfg(feature="registration")]
#[test]
fn table_reduces_ingested_classes_with_exact_coverage_and_poisoned_failures() {
    use serde_json::json;
    let plan=json!({"source":"events","relational":{"selection":[
        {"field":"duration","alias":"duration"},
        {"function":"count","alias":"count"},
        {"field":"duration","function":"sum","alias":"sum"},
        {"field":"duration","function":"avg","alias":"avg"}],"groupBy":["duration"]}});
    let coverage=||vec![Coverage{source:"test".into(),definition:"rx".into(),time:0,count:2}];
    let mut table=SummaryTable::new(&plan,coverage(),0,86_400_000).unwrap();
    table.read(&mut reader(),"test","rx").unwrap();
    let result=table.finish().unwrap();
    assert_eq!(result["rows"],json!([
        [{"$integer":"10"},{"$integer":"1"},{"$integer":"10"},10.0],
        [{"$integer":"30"},{"$integer":"1"},{"$integer":"30"},30.0]
    ]));
    assert!(table.finish().is_err());
    let mut table=SummaryTable::new(&plan,coverage(),0,86_400_000).unwrap();
    assert!(table.finish().is_err());
    table.read(&mut reader(),"test","rx").unwrap();
    assert!(table.read(&mut reader(),"test","rx").is_err());
    assert!(table.finish().is_err());
    let mut bad=coverage();bad[0].count=3;
    let mut table=SummaryTable::new(&plan,bad,0,86_400_000).unwrap();
    assert!(table.read(&mut reader(),"test","rx").is_err());
    assert!(table.finish().is_err());
}

#[cfg(feature="registration")]
#[test]
fn table_admission_failure_is_not_a_partial_result() {
    let plan=serde_json::json!({"source":"summaries","relational":{"selection":[],"groupBy":null}});
    let budget=MemoryBudget::new(128);
    let mut table=SummaryTable::new(&plan,vec![Coverage{source:"test".into(),definition:"rx".into(),time:0,count:2}],0,86_400_000).unwrap().memory_budget(budget.clone());
    assert!(table.read(&mut reader(),"test","rx").is_err());
    assert!(table.finish().is_err());drop(table);assert_eq!(budget.used(),0);
}

#[cfg(feature="registration")]
#[test]
fn table_only_reduces_timeline_selected_hours() {
    let plan=serde_json::json!({"source":"summaries","relational":{"selection":[{"field":"count","function":"sum","alias":"count"}],"groupBy":[]}});
    let mut table=SummaryTable::new(&plan,vec![Coverage{source:"test".into(),definition:"rx".into(),time:3_600_000,count:0}],0,86_400_000).unwrap();
    table.read(&mut reader(),"test","rx").unwrap();
    assert_eq!(table.finish().unwrap()["rows"],serde_json::json!([[{"$integer":"0"}]]));
}

#[cfg(feature="query-language")]
#[test]
fn sql_summary_table_binds_text_literals_to_source_units() {
    use serde_json::json;
    let plan=data_seeds::language::parse("SELECT * FROM summaries WHERE \"duration.max\" > '0.02 ms'", &["duration.max".into()]).unwrap();
    let plan=&plan["plan"];
    let mut table=SummaryTable::new(plan,vec![Coverage{source:"test".into(),definition:"rx".into(),time:0,count:2}],0,86_400_000).unwrap();
    table.read(&mut reader(),"test","rx").unwrap();
    let result=table.finish().unwrap();
    assert_eq!(result["rows"].as_array().unwrap().len(),1,"{result}");
    assert!(result.to_string().contains("30"));
    // The old microsecond assumption must not leak into numeric string parsing.
    assert_ne!(result, json!(null));
}
