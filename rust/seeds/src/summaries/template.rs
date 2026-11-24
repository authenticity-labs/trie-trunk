//! Reusable statistical structures. Types and counts are PAC plan parameters.
use crate::{Document, Error, Field, doc};

pub fn summary_template() -> Result<Document, Error> {
    let count = |name| Field::new(name).r#type("uint").size(8);
    let parameter = |name| Field::new(name).r#type("any");
    let mut library = doc::new("summary").field(count("count"));
    for class in ["numeric", "real", "temporal", "boolean", "categories"] {
        library = library.field(Field::new(class).array_object(class));
    }
    library = library.field(Field::new("counts").array("uint").element_size(3));
    // Floating accumulators retain their compensation state; integer widths
    // and signedness are parameters, never additional authored Definitions.
    for class in ["numeric", "real", "temporal"] {
        library = library
            .definition(class)
            .field(count("count"))
            .field(count("null_count"))
            .field(parameter("min"))
            .field(parameter("max"));
        if class != "temporal" {
            library = library
                .field(parameter("sum"))
                .field(count("nonfinite_count"))
                .field(Field::new("overflow").r#type("bool"));
            if class == "real" {
                library = library.field(Field::new("compensation").r#type("float").size(8));
            }
        }
    }
    library
        .definition("boolean")
        .field(count("count"))
        .field(count("null_count"))
        .field(count("true_count"))
        .definition("categories")
        .field(count("count"))
        .field(count("null_count"))
        .select(0)
        .sign()
}
