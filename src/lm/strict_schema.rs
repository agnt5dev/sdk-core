//! Make a JSON schema acceptable to OpenAI's strict structured outputs.
//!
//! With `strict: true`, OpenAI accepts only a subset of JSON Schema: every
//! object sets `additionalProperties: false` and lists all of its properties
//! in `required`, `allOf` isn't allowed, and a `$ref` can't have sibling
//! keywords. Pydantic models and hand-written schemas rarely meet all of that,
//! so a strict request with them fails with a 400 before the model runs.
//!
//! The rewrite follows openai-python's `to_strict_json_schema`. It never
//! widens what a schema accepts, so an answer still validates against the
//! caller's schema. A property that was optional becomes required with its own
//! type: to let the model leave it empty, its type has to allow `null`
//! (`Optional[str] = None` in Pydantic, `["string", "null"]` by hand).
//! Free-form objects such as `dict` fields have no strict form, so they are
//! left for OpenAI to reject rather than narrowed to `{}`.

use serde_json::{Map, Value};

use super::interface::JsonSchemaFormat;

/// The schema to send for `format`: rewritten for strict mode when the format
/// asks for it, unchanged otherwise.
pub(crate) fn request_schema(format: &JsonSchemaFormat) -> Value {
    if format.strict {
        to_strict_json_schema(&format.schema)
    } else {
        format.schema.clone()
    }
}

/// Rewrites `schema` into the subset OpenAI accepts with `strict: true`.
pub(crate) fn to_strict_json_schema(schema: &Value) -> Value {
    let mut strict = schema.clone();
    make_strict(&mut strict, schema, &mut Vec::new());
    strict
}

/// `root` is the caller's schema, which `$ref`s resolve against. `inlining`
/// holds the refs inlined on the way down to this node, to stop at recursion.
fn make_strict(node: &mut Value, root: &Value, inlining: &mut Vec<String>) {
    let Value::Object(schema) = node else {
        return;
    };
    let depth = inlining.len();
    flatten(schema, root, inlining);

    let required = match schema.get_mut("properties") {
        Some(Value::Object(properties)) => {
            for property in properties.values_mut() {
                make_strict(property, root, inlining);
            }
            Some(properties.keys().cloned().map(Value::String).collect())
        }
        _ => None,
    };
    if let Some(required) = required {
        schema.insert("required".to_string(), Value::Array(required));
        schema
            .entry("additionalProperties")
            .or_insert(Value::Bool(false));
    }

    for keyword in ["items", "prefixItems", "anyOf", "oneOf", "allOf"] {
        match schema.get_mut(keyword) {
            Some(Value::Array(entries)) => {
                for entry in entries {
                    make_strict(entry, root, inlining);
                }
            }
            Some(entry @ Value::Object(_)) => make_strict(entry, root, inlining),
            _ => {}
        }
    }
    for keyword in ["$defs", "definitions"] {
        if let Some(Value::Object(definitions)) = schema.get_mut(keyword) {
            for definition in definitions.values_mut() {
                make_strict(definition, root, inlining);
            }
        }
    }

    inlining.truncate(depth);
}

/// Unwraps a one-entry `allOf` (older Pydantic wraps a `$ref` in one to give
/// it a description) and inlines a `$ref` that has sibling keywords. The
/// node's own keywords win over the ones pulled in.
fn flatten(schema: &mut Map<String, Value>, root: &Value, inlining: &mut Vec<String>) {
    loop {
        if let Some(Value::Array(all_of)) = schema.get("allOf") {
            if let [Value::Object(entry)] = all_of.as_slice() {
                let entry = entry.clone();
                schema.remove("allOf");
                merge_missing(schema, entry);
                continue;
            }
        }

        if schema.len() > 1 {
            if let Some(Value::String(reference)) = schema.get("$ref") {
                let reference = reference.clone();
                if inlining.contains(&reference) {
                    // A recursive definition can't be inlined, but a bare
                    // `$ref` is allowed.
                    schema.retain(|key, _| key == "$ref");
                } else if let Some(Value::Object(target)) = resolve_ref(root, &reference) {
                    schema.remove("$ref");
                    merge_missing(schema, target.clone());
                    inlining.push(reference);
                    continue;
                }
            }
        }

        return;
    }
}

/// Resolves a local `$ref` such as `#/$defs/City` against the root schema.
fn resolve_ref<'a>(root: &'a Value, reference: &str) -> Option<&'a Value> {
    root.pointer(reference.strip_prefix('#')?)
}

/// Copies the keywords `schema` doesn't already have. Definitions stay at the
/// root, where refs resolve.
fn merge_missing(schema: &mut Map<String, Value>, from: Map<String, Value>) {
    for (key, value) in from {
        if key != "$defs" && key != "definitions" {
            schema.entry(key).or_insert(value);
        }
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    /// What Pydantic 2 emits for a model with a nested model, an optional
    /// nested model, an optional field, a defaulted field and a
    /// `default_factory` field.
    fn pydantic_person() -> Value {
        json!({
            "$defs": {
                "City": {
                    "description": "A city.",
                    "properties": {
                        "name": {"title": "Name", "type": "string"},
                        "country": {"title": "Country", "type": "string"}
                    },
                    "required": ["name", "country"],
                    "title": "City",
                    "type": "object"
                }
            },
            "properties": {
                "name": {"title": "Name", "type": "string"},
                "age": {"default": 0, "title": "Age", "type": "integer"},
                "email": {
                    "anyOf": [{"type": "string"}, {"type": "null"}],
                    "default": null,
                    "title": "Email"
                },
                "tags": {"items": {"type": "string"}, "title": "Tags", "type": "array"},
                "home": {"$ref": "#/$defs/City", "description": "Where they live"},
                "previous": {
                    "anyOf": [{"$ref": "#/$defs/City"}, {"type": "null"}],
                    "default": null
                }
            },
            "required": ["name", "home"],
            "title": "Person",
            "type": "object"
        })
    }

    fn strict_city() -> Value {
        json!({
            "description": "A city.",
            "properties": {
                "name": {"title": "Name", "type": "string"},
                "country": {"title": "Country", "type": "string"}
            },
            "required": ["country", "name"],
            "title": "City",
            "type": "object",
            "additionalProperties": false
        })
    }

    #[test]
    fn plain_object_lists_every_property_and_closes_the_object() {
        // The TypeScript SDK's own docs example.
        let schema = json!({
            "type": "object",
            "properties": {
                "name": {"type": "string"},
                "age": {"type": "number"},
                "email": {"type": "string", "format": "email"}
            },
            "required": ["name", "age"]
        });

        assert_eq!(
            to_strict_json_schema(&schema),
            json!({
                "type": "object",
                "properties": {
                    "name": {"type": "string"},
                    "age": {"type": "number"},
                    "email": {"type": "string", "format": "email"}
                },
                "required": ["age", "email", "name"],
                "additionalProperties": false
            })
        );
    }

    #[test]
    fn pydantic_model_with_nested_and_optional_fields() {
        let strict = to_strict_json_schema(&pydantic_person());

        assert_eq!(strict["additionalProperties"], false);
        assert_eq!(
            strict["required"],
            json!(["age", "email", "home", "name", "previous", "tags"])
        );
        assert_eq!(strict["$defs"]["City"], strict_city());

        let properties = &strict["properties"];
        // Optional fields keep their own type, defaults included.
        assert_eq!(
            properties["age"],
            json!({"default": 0, "title": "Age", "type": "integer"})
        );
        assert_eq!(
            properties["tags"],
            json!({"items": {"type": "string"}, "title": "Tags", "type": "array"})
        );
        assert_eq!(
            properties["email"],
            pydantic_person()["properties"]["email"]
        );
        // A nullable nested model keeps its bare `$ref`.
        assert_eq!(
            properties["previous"],
            pydantic_person()["properties"]["previous"]
        );
        // A `$ref` with a description is inlined, keeping the description.
        let mut home = strict_city();
        home["description"] = json!("Where they live");
        assert_eq!(properties["home"], home);
    }

    #[test]
    fn one_entry_all_of_is_unwrapped() {
        let schema = json!({
            "type": "object",
            "properties": {
                "home": {"allOf": [{"$ref": "#/$defs/City"}], "description": "Where they live"},
                "work": {"allOf": [{"$ref": "#/$defs/City"}]}
            },
            "$defs": {"City": pydantic_person()["$defs"]["City"]}
        });

        let strict = to_strict_json_schema(&schema);

        let mut home = strict_city();
        home["description"] = json!("Where they live");
        assert_eq!(strict["properties"]["home"], home);
        assert_eq!(
            strict["properties"]["work"],
            json!({"$ref": "#/$defs/City"})
        );
    }

    #[test]
    fn root_ref_with_definitions_is_inlined() {
        // zod-to-json-schema's default output.
        let schema = json!({
            "$ref": "#/definitions/Answer",
            "definitions": {
                "Answer": {
                    "type": "object",
                    "properties": {"city": {"type": "string"}},
                    "required": ["city"]
                }
            },
            "$schema": "http://json-schema.org/draft-07/schema#"
        });

        let strict = to_strict_json_schema(&schema);

        assert!(strict.get("$ref").is_none());
        assert_eq!(strict["type"], "object");
        assert_eq!(strict["required"], json!(["city"]));
        assert_eq!(strict["additionalProperties"], false);
        assert_eq!(
            strict["definitions"]["Answer"]["additionalProperties"],
            false
        );
    }

    #[test]
    fn arrays_and_unions_are_walked() {
        let schema = json!({
            "type": "object",
            "properties": {
                "items": {
                    "type": "array",
                    "items": {"type": "object", "properties": {"sku": {"type": "string"}}}
                },
                "pair": {
                    "type": "array",
                    "prefixItems": [{"type": "object", "properties": {"a": {"type": "string"}}}]
                },
                "contact": {
                    "anyOf": [
                        {"type": "object", "properties": {"email": {"type": "string"}}},
                        {"type": "null"}
                    ]
                }
            }
        });

        let strict = to_strict_json_schema(&schema);
        let properties = &strict["properties"];

        for object in [
            &properties["items"]["items"],
            &properties["pair"]["prefixItems"][0],
            &properties["contact"]["anyOf"][0],
        ] {
            assert_eq!(object["additionalProperties"], false, "{object}");
            assert_eq!(object["required"].as_array().unwrap().len(), 1, "{object}");
        }
        assert_eq!(properties["contact"]["anyOf"][1], json!({"type": "null"}));
    }

    #[test]
    fn strict_schemas_are_unchanged() {
        let schema = json!({
            "type": "object",
            "properties": {
                "city": {"type": "string"},
                "country": {"type": ["string", "null"]}
            },
            "required": ["city", "country"],
            "additionalProperties": false
        });
        assert_eq!(to_strict_json_schema(&schema), schema);

        let once = to_strict_json_schema(&pydantic_person());
        assert_eq!(to_strict_json_schema(&once), once);
    }

    #[test]
    fn free_form_objects_are_left_alone() {
        let schema = json!({
            "type": "object",
            "properties": {
                "meta": {"type": "object", "additionalProperties": true},
                "counts": {"type": "object", "additionalProperties": {"type": "integer"}},
                "anything": {"type": "object"}
            },
            "required": ["meta", "counts", "anything"]
        });

        let strict = to_strict_json_schema(&schema);

        for name in ["meta", "counts", "anything"] {
            assert_eq!(
                strict["properties"][name], schema["properties"][name],
                "{name}"
            );
        }
    }

    #[test]
    fn recursive_ref_with_siblings_keeps_a_bare_ref() {
        let schema = json!({
            "$ref": "#/$defs/Node",
            "$defs": {
                "Node": {
                    "type": "object",
                    "properties": {
                        "value": {"type": "string"},
                        "next": {"$ref": "#/$defs/Node", "description": "The next node"}
                    }
                }
            }
        });

        let strict = to_strict_json_schema(&schema);

        assert_eq!(
            strict["properties"]["next"],
            json!({"$ref": "#/$defs/Node"})
        );
        assert_eq!(strict["$defs"]["Node"]["additionalProperties"], false);
    }

    #[test]
    fn non_strict_formats_are_sent_as_given() {
        let format = JsonSchemaFormat::new("person", pydantic_person());
        assert_eq!(
            request_schema(&format),
            to_strict_json_schema(&pydantic_person())
        );

        let format = format.with_strict(false);
        assert_eq!(request_schema(&format), pydantic_person());
    }
}
