use data_seeds::{doc,Field,SeedDefinition,summaries::SummaryDefinitions,language,relational};
use serde_json::{json,Value};
fn main(){
 let source=SeedDefinition::prescribe(&doc::new("audit").field(Field::new("duration").r#type("uint").size(4).annotation("#summ(numeric)").annotation("@unit(ms)")).sign().unwrap()).unwrap();
 let mut library=SummaryDefinitions::new();let plan=library.prescribe(source,288).unwrap();
 let mut results=vec![json!({"case":"definition_count","reported_len":library.len(),"actual_definitions":plan.document().definitions().len(),"root_fields":plan.document().definitions()[0].fields.len()})];
 let query=|value:Value| json!({"source":"summaries","relational":{"selection":[{"field":"duration","function":null,"alias":"duration"}]},"filters":[{"field":"duration","op":">=","value":value}]});
 let rows=[json!({"count":1,"values":{"duration":2}})];
 results.push(json!({"case":"millisecond_source_duration_literal","source_annotation":"@unit(ms)","stored_value":2,"unit_literal":relational::reduce(&query(json!({"kind":"duration","value":"1ms"})),&rows),"equivalent_numeric_literal":relational::reduce(&query(json!({"kind":"number","value":"1"})),&rows)}));
 let sql=language::parse("SELECT duration FROM summaries WHERE NOT (duration >= 1)",&["duration".into()]).unwrap();
 results.push(json!({"case":"sql_null_negation_control","result":relational::reduce(&sql["plan"],&[json!({"count":1,"values":{"duration":null}})])}));
 let sql=language::parse("SELECT MIN(duration) FROM summaries GROUP BY bucket",&["duration".into(),"bucket".into()]).unwrap();
 results.push(json!({"case":"unsigned_16_byte_min","value":u128::MAX.to_string(),"result":relational::reduce(&sql["plan"],&[json!({"count":1,"values":{"bucket":0,"duration":{"$integer":u128::MAX.to_string()}}})])}));
 let mut accumulator=plan.accumulator(0).unwrap();
 accumulator.ingest(0,(),|(),pill| {pill.set_uint("duration",2u32)?;Ok(())}).unwrap();
 accumulator.complete(accumulator.end()).unwrap();
 let bytes=accumulator.seal().unwrap().bytes().to_vec();
 for (name,value) in [("unit",json!({"kind":"duration","value":"1ms"})),("numeric",json!({"kind":"number","value":"1"}))] {
   let q=json!({"source":"summaries","relational":{"selection":[{"field":"duration.min","function":null,"alias":"min"}]},"filters":[{"field":"duration.min","op":">=","value":value}]});
   let mut reader=data_seeds::summaries::SummaryReader::pack(plan.layout().clone(),bytes.clone(),0).unwrap();
   let mut table=data_seeds::summaries::SummaryTable::new(&q,vec![data_seeds::summaries::Coverage{source:"audit".into(),definition:"audit".into(),time:0,count:1}],0,300000).unwrap();
   table.read(&mut reader,"audit","audit").unwrap();
   results.push(json!({"case":format!("summary_table_ms_{name}"),"result":table.finish().unwrap()}));
 }
 println!("{}",serde_json::to_string_pretty(&results).unwrap());
}
