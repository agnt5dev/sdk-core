//! Make a JSON schema acceptable to OpenAI's strict structured outputs.
//!
//! With `strict: true`, OpenAI accepts only a subset of JSON Schema: every
//! object sets `additionalProperties: false` and lists all of its properties
//! in `required`, `allOf` isn't allowed, and a `$ref` can't have sibling
//! keywords. Pydantic models and hand-written schemas rarely meet all of that,
//! so a strict request with them fails with a 400 before the model runs.
//!
//! The rewrite keeps what a schema means, so an answer validates against the
//! caller's schema, with one exception noted below:
//!
//! - An object that declares `properties` is closed, and all of them are
//!   listed in `required`. An optional property keeps its own type, so the
//!   model fills it in; with a default, the default says what empty looks
//!   like. The exception is an optional scalar with no default: it also
//!   allows `null`, OpenAI's way of writing optional, so the model can say a
//!   value isn't there instead of making one up. Arrays and objects can be
//!   empty instead, and a Pydantic `default_factory` field must not get
//!   `null`.
//! - A one-entry `allOf`, and a `$ref` with sibling keywords, become a
//!   one-entry `anyOf`, which means the same thing.
//! - A `$ref` at the root, which Pydantic emits for recursive models and
//!   zod-to-json-schema for every model, is replaced by the definition it
//!   points to, since OpenAI wants an object there.
//!
//! Free-form objects such as `dict` fields have no strict form, so they are
//! left for OpenAI to reject rather than narrowed to `{}`.

use serde_json::{json, Map, Value};

use super::interface::JsonSchemaFormat;

/// Keywords that describe a schema without constraining what it accepts.
const ANNOTATIONS: [&str; 8] = [
    "title",
    "description",
    "default",
    "examples",
    "$comment",
    "deprecated",
    "readOnly",
    "writeOnly",
];

/// Keywords OpenAI accepts next to a `$ref`.
const REF_SIBLINGS: [&str; 3] = ["$defs", "definitions", "$schema"];

const SCALAR_TYPES: [&str; 4] = ["string", "number", "integer", "boolean"];

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
    if let Value::Object(root) = &mut strict {
        inline_root_ref(root, schema);
    }
    make_strict(&mut strict);
    strict
}

/// Replaces a root `$ref` with the definition it points to. Only annotations,
/// the definitions themselves and keywords the definition repeats (the Python
/// SDK adds `"type": "object"`) may sit next to it, so nothing that constrains
/// the answer is lost. The definition stays in place for any recursive refs
/// to it.
fn inline_root_ref(root: &mut Map<String, Value>, schema: &Value) {
    // A ref can point at another ref; follow a short chain.
    for _ in 0..8 {
        let Some(Value::String(reference)) = root.get("$ref") else {
            return;
        };
        let Some(Value::Object(target)) = reference
            .strip_prefix('#')
            .and_then(|pointer| schema.pointer(pointer))
        else {
            return;
        };
        let nothing_lost = root.iter().all(|(key, value)| {
            key == "$ref"
                || REF_SIBLINGS.contains(&key.as_str())
                || ANNOTATIONS.contains(&key.as_str())
                || target.get(key) == Some(value)
        });
        if !nothing_lost {
            return;
        }
        let target = target.clone();
        root.remove("$ref");
        for (key, value) in target {
            if !REF_SIBLINGS.contains(&key.as_str()) {
                root.entry(key).or_insert(value);
            }
        }
    }
}

fn make_strict(node: &mut Value) {
    let Value::Object(schema) = node else {
        return;
    };
    wrap_in_any_of(schema);

    let was_required = |name: &String| {
        schema
            .get("required")
            .and_then(Value::as_array)
            .is_some_and(|required| required.iter().any(|entry| entry == name))
    };
    let optional: Vec<String> = match schema.get("properties") {
        Some(Value::Object(properties)) => properties
            .keys()
            .filter(|name| !was_required(name))
            .cloned()
            .collect(),
        _ => Vec::new(),
    };
    let required = match schema.get_mut("properties") {
        Some(Value::Object(properties)) => {
            for (name, property) in properties.iter_mut() {
                if optional.contains(name) {
                    allow_null_for_optional_scalar(property);
                }
                make_strict(property);
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
            Some(Value::Array(entries)) => entries.iter_mut().for_each(make_strict),
            Some(entry @ Value::Object(_)) => make_strict(entry),
            _ => {}
        }
    }
    for keyword in ["$defs", "definitions"] {
        if let Some(Value::Object(definitions)) = schema.get_mut(keyword) {
            definitions.values_mut().for_each(make_strict);
        }
    }
}

/// Lets an optional scalar with no default answer `null`. See the module docs.
fn allow_null_for_optional_scalar(property: &mut Value) {
    let Value::Object(schema) = property else {
        return;
    };
    let types: Vec<&str> = match schema.get("type") {
        Some(Value::String(name)) => vec![name.as_str()],
        Some(Value::Array(names)) => names.iter().filter_map(Value::as_str).collect(),
        _ => Vec::new(),
    };
    let listed = schema.contains_key("enum") || schema.contains_key("const");
    let scalar = if types.is_empty() {
        listed
    } else {
        types.iter().all(|name| SCALAR_TYPES.contains(name))
    };
    if !scalar || schema.contains_key("default") {
        return;
    }
    if listed {
        // `null` has to get past the enum too.
        let inner = std::mem::take(property);
        *property = json!({ "anyOf": [inner, { "type": "null" }] });
    } else {
        let mut nullable: Vec<Value> = types.into_iter().map(Value::from).collect();
        nullable.push(Value::from("null"));
        schema.insert("type".to_string(), Value::Array(nullable));
    }
}

/// Turns a one-entry `allOf` (older Pydantic wraps a `$ref` in one to give it
/// a description), or a `$ref` with sibling keywords, into a one-entry
/// `anyOf`. The siblings stay where they are, so every constraint still
/// applies.
fn wrap_in_any_of(schema: &mut Map<String, Value>) {
    if schema.contains_key("anyOf") {
        return;
    }
    if matches!(schema.get("allOf"), Some(Value::Array(entries)) if entries.len() == 1) {
        let entries = schema.remove("allOf").unwrap_or_default();
        schema.insert("anyOf".to_string(), entries);
    } else if schema.contains_key("$ref")
        && schema
            .keys()
            .any(|key| key != "$ref" && !REF_SIBLINGS.contains(&key.as_str()))
    {
        let reference = schema.remove("$ref").unwrap_or_default();
        schema.insert("anyOf".to_string(), json!([{ "$ref": reference }]));
    }
}

#[cfg(test)]
mod tests {
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
                    "email": {"type": ["string", "null"], "format": "email"}
                },
                "required": ["age", "email", "name"],
                "additionalProperties": false
            })
        );
    }

    #[test]
    fn pydantic_model_with_nested_and_optional_fields() {
        let person = pydantic_person();
        let strict = to_strict_json_schema(&person);

        assert_eq!(strict["additionalProperties"], false);
        assert_eq!(
            strict["required"],
            json!(["age", "email", "home", "name", "previous", "tags"])
        );
        let city = &strict["$defs"]["City"];
        assert_eq!(city["additionalProperties"], false);
        assert_eq!(city["required"], json!(["country", "name"]));

        let properties = &strict["properties"];
        // Optional fields keep their own type, defaults included.
        for name in ["age", "tags", "email", "previous"] {
            assert_eq!(properties[name], person["properties"][name], "{name}");
        }
        // A `$ref` with a description moves into a one-entry `anyOf`.
        assert_eq!(
            properties["home"],
            json!({"anyOf": [{"$ref": "#/$defs/City"}], "description": "Where they live"})
        );
    }

    #[test]
    fn optional_scalars_without_a_default_may_be_null() {
        let schema = json!({
            "type": "object",
            "properties": {
                "email": {"type": "string", "format": "email"},
                "score": {"type": ["integer", "number"]},
                "tier": {"enum": ["gold", "silver"]},
                "nickname": {"type": "string", "default": ""},
                "tags": {"type": "array", "items": {"type": "string"}},
                "address": {
                    "type": "object",
                    "properties": {"city": {"type": "string"}},
                    "required": ["city"]
                },
                "name": {"type": "string"}
            },
            "required": ["name"]
        });

        let properties = to_strict_json_schema(&schema)["properties"].clone();

        assert_eq!(
            properties["email"],
            json!({"type": ["string", "null"], "format": "email"})
        );
        assert_eq!(
            properties["score"]["type"],
            json!(["integer", "number", "null"])
        );
        assert_eq!(
            properties["tier"],
            json!({"anyOf": [{"enum": ["gold", "silver"]}, {"type": "null"}]})
        );
        // A default, an array, an object or a required field keeps its type.
        for name in ["nickname", "tags", "name"] {
            assert_eq!(properties[name], schema["properties"][name], "{name}");
        }
        assert_eq!(properties["address"]["type"], "object");
    }

    #[test]
    fn root_ref_with_a_conflicting_sibling_is_left_for_openai() {
        let schema = json!({
            "$ref": "#/$defs/A",
            "$defs": {"A": {"type": "object", "properties": {"a": {"type": "string"}}}},
            "type": "array"
        });

        let strict = to_strict_json_schema(&schema);

        assert_eq!(strict["anyOf"], json!([{"$ref": "#/$defs/A"}]));
        assert_eq!(strict["type"], "array");
    }

    #[test]
    fn constraints_next_to_all_of_and_ref_still_apply() {
        let schema = json!({
            "type": "object",
            "properties": {
                "status": {
                    "allOf": [{"type": "string", "enum": ["approved"]}],
                    "enum": ["approved", "other"]
                },
                "children": {
                    "type": "array",
                    "items": {"$ref": "#/$defs/Node", "minItems": 1}
                },
                "home": {"allOf": [{"$ref": "#/$defs/Node"}], "description": "Where they live"}
            },
            "required": ["status", "children", "home"],
            "$defs": {"Node": {"type": ["array", "null"]}}
        });

        let properties = to_strict_json_schema(&schema)["properties"].clone();

        assert_eq!(
            properties["status"],
            json!({
                "anyOf": [{"type": "string", "enum": ["approved"]}],
                "enum": ["approved", "other"]
            })
        );
        assert_eq!(
            properties["children"]["items"],
            json!({"anyOf": [{"$ref": "#/$defs/Node"}], "minItems": 1})
        );
        assert_eq!(
            properties["home"],
            json!({"anyOf": [{"$ref": "#/$defs/Node"}], "description": "Where they live"})
        );
    }

    #[test]
    fn root_ref_is_replaced_by_its_definition() {
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
        assert_eq!(strict["$schema"], schema["$schema"]);
        assert_eq!(
            strict["definitions"]["Answer"]["additionalProperties"],
            false
        );
    }

    #[test]
    fn recursive_pydantic_model_keeps_its_definition_for_the_recursion() {
        // Pydantic's output for a model that contains a list of itself.
        let schema = json!({
            "$defs": {
                "Node": {
                    "description": "A person and their reports.",
                    "properties": {
                        "name": {"title": "Name", "type": "string"},
                        "reports": {
                            "description": "Direct reports",
                            "items": {"$ref": "#/$defs/Node"},
                            "title": "Reports",
                            "type": "array"
                        }
                    },
                    "required": ["name"],
                    "title": "Node",
                    "type": "object"
                }
            },
            "$ref": "#/$defs/Node",
            "type": "object"
        });

        let strict = to_strict_json_schema(&schema);

        assert!(strict.get("$ref").is_none());
        assert_eq!(strict["type"], "object");
        assert_eq!(strict["required"], json!(["name", "reports"]));
        assert_eq!(
            strict["properties"]["reports"]["items"],
            json!({"$ref": "#/$defs/Node"})
        );
        assert_eq!(strict["$defs"]["Node"]["additionalProperties"], false);
        assert_eq!(
            strict["$defs"]["Node"]["required"],
            json!(["name", "reports"])
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
    fn shared_and_recursive_refs_stay_references() {
        // Each layer refers to the one before it twice, with a description.
        let mut definitions = Map::new();
        definitions.insert(
            "L0".to_string(),
            json!({"type": "object", "properties": {"v": {"type": "string"}}}),
        );
        for layer in 1..12 {
            let previous = format!("#/$defs/L{}", layer - 1);
            definitions.insert(
                format!("L{layer}"),
                json!({
                    "type": "object",
                    "properties": {
                        "left": {"$ref": previous, "description": "left"},
                        "right": {"$ref": previous, "description": "right"},
                        "self": {"$ref": format!("#/$defs/L{layer}"), "description": "itself"}
                    }
                }),
            );
        }
        let schema = json!({"$ref": "#/$defs/L11", "$defs": definitions});

        let strict = to_strict_json_schema(&schema);

        assert!(strict.to_string().len() < 2 * schema.to_string().len());
        assert_eq!(
            strict["$defs"]["L11"]["properties"]["self"],
            json!({"anyOf": [{"$ref": "#/$defs/L11"}], "description": "itself"})
        );
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
