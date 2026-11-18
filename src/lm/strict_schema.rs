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
//!   like. The exception is where `null`, OpenAI's way of writing optional, is
//!   the only honest way to leave a value out, so it's allowed too: an
//!   optional scalar with no default, where the model would otherwise make a
//!   value up, and an optional property that refers back into a recursive
//!   definition, where the recursion could otherwise never end. Arrays and
//!   objects can be empty instead, and a Pydantic `default_factory` field must
//!   not get `null`. A property that a `$ref` points at keeps its type, since
//!   the fields that refer to it may be required.
//! - A one-entry `allOf`, and a `$ref` with sibling keywords, become a
//!   one-entry `anyOf`, which means the same thing. An `allOf` that a `$ref`
//!   points inside stays, for OpenAI to report: renaming it would leave that
//!   reference pointing nowhere.
//! - A `$ref` at the root, which Pydantic emits for recursive models and
//!   zod-to-json-schema for every model, is replaced by the definition it
//!   points to, since OpenAI wants an object there.
//!
//! Free-form objects such as `dict` fields have no strict form, so they are
//! left for OpenAI to reject rather than narrowed to `{}`.
//!
//! OpenAI holds a strict tool's parameters to the same rules, so those are
//! rewritten the same way.

use std::collections::HashSet;

use serde_json::{json, Map, Value};

use super::interface::{JsonSchemaFormat, ToolDefinition};

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

/// A tool's parameters to send: rewritten for strict mode when the tool asks
/// for it, unchanged otherwise.
pub(crate) fn tool_parameters(tool: &ToolDefinition) -> Option<Value> {
    match (&tool.parameters, tool.strict) {
        (Some(parameters), Some(true)) => Some(to_strict_json_schema(parameters)),
        (parameters, _) => parameters.clone(),
    }
}

/// Rewrites `schema` into the subset OpenAI accepts with `strict: true`.
pub(crate) fn to_strict_json_schema(schema: &Value) -> Value {
    let mut strict = schema.clone();
    if let Value::Object(root) = &mut strict {
        inline_root_ref(root, schema);
    }
    make_strict(&mut strict, "", &Refs::of(schema));
    strict
}

/// What the rewrite needs to know about the caller's local `$ref`s.
struct Refs {
    /// The JSON pointers they point at, such as `/$defs/City`.
    targets: HashSet<String>,
    /// The targets on a reference cycle: following the refs inside one leads
    /// back to it.
    recursive: HashSet<String>,
}

impl Refs {
    fn of(schema: &Value) -> Self {
        let mut targets = HashSet::new();
        collect_ref_targets(schema, &mut targets);
        let recursive = targets
            .iter()
            .filter(|target| leads_back(schema, target, target, &mut HashSet::new()))
            .cloned()
            .collect();
        Self { targets, recursive }
    }

    /// Whether a `$ref` points at the schema at `path`, or inside it.
    fn point_into(&self, path: &str) -> bool {
        self.targets
            .iter()
            .any(|target| target == path || target.starts_with(&format!("{path}/")))
    }

    fn is_recursive(&self, reference: &str) -> bool {
        reference
            .strip_prefix('#')
            .is_some_and(|pointer| self.recursive.contains(pointer))
    }
}

/// Whether following the `$ref`s inside the schema at `from` leads to
/// `target`.
fn leads_back(schema: &Value, from: &str, target: &str, seen: &mut HashSet<String>) -> bool {
    let Some(node) = schema.pointer(from) else {
        return false;
    };
    let mut next = HashSet::new();
    collect_ref_targets(node, &mut next);
    next.into_iter().any(|pointer| {
        pointer == target
            || (seen.insert(pointer.clone()) && leads_back(schema, &pointer, target, seen))
    })
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

/// `path` is the node's JSON pointer in the caller's schema.
fn make_strict(node: &mut Value, path: &str, refs: &Refs) {
    let Value::Object(schema) = node else {
        return;
    };
    wrap_in_any_of(schema, path, refs);

    let originally_required: Vec<Value> = schema
        .get("required")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let required = match schema.get_mut("properties") {
        Some(Value::Object(properties)) => {
            // A required name with no property of its own has no strict form:
            // closing the object would forbid it. Leave the object as it is.
            let closable = originally_required.iter().all(|name| {
                name.as_str()
                    .is_some_and(|name| properties.contains_key(name))
            });
            for (name, property) in properties.iter_mut() {
                let property_path = format!("{path}/properties/{}", escape_pointer(name));
                let optional = !originally_required.iter().any(|entry| entry == name);
                // A `$ref` elsewhere may need this schema without `null`.
                if closable && optional && !refs.point_into(&property_path) {
                    allow_null_for_optional_scalar(property);
                    allow_null_for_optional_recursion(property, refs);
                }
                make_strict(property, &property_path, refs);
            }
            closable.then(|| properties.keys().cloned().map(Value::String).collect())
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
                for (index, entry) in entries.iter_mut().enumerate() {
                    make_strict(entry, &format!("{path}/{keyword}/{index}"), refs);
                }
            }
            Some(entry @ Value::Object(_)) => {
                make_strict(entry, &format!("{path}/{keyword}"), refs)
            }
            _ => {}
        }
    }
    for keyword in ["$defs", "definitions"] {
        if let Some(Value::Object(definitions)) = schema.get_mut(keyword) {
            for (name, definition) in definitions.iter_mut() {
                let definition_path = format!("{path}/{keyword}/{}", escape_pointer(name));
                make_strict(definition, &definition_path, refs);
            }
        }
    }
}

/// The JSON pointers that local `$ref`s point at, such as `/$defs/City`.
fn collect_ref_targets(node: &Value, targets: &mut HashSet<String>) {
    match node {
        Value::Object(schema) => {
            if let Some(Value::String(reference)) = schema.get("$ref") {
                if let Some(pointer) = reference.strip_prefix('#') {
                    targets.insert(pointer.to_string());
                }
            }
            for value in schema.values() {
                collect_ref_targets(value, targets);
            }
        }
        Value::Array(entries) => {
            for entry in entries {
                collect_ref_targets(entry, targets);
            }
        }
        _ => {}
    }
}

/// Escapes a key for use in a JSON pointer.
fn escape_pointer(key: &str) -> String {
    key.replace('~', "~0").replace('/', "~1")
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
    // The values an `enum` or `const` allows.
    let values: Vec<&Value> = match (schema.get("enum"), schema.get("const")) {
        (Some(Value::Array(values)), _) => values.iter().collect(),
        (_, Some(value)) => vec![value],
        _ => Vec::new(),
    };
    let listed = !values.is_empty();
    let scalar = if types.is_empty() {
        // Without a `type`, the values say what kind of value it is.
        listed
            && values
                .iter()
                .all(|value| value.is_string() || value.is_number() || value.is_boolean())
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

/// Lets an optional property that refers back into a recursive definition,
/// directly or through a one-entry `allOf` or `anyOf`, answer `null`. Once
/// the property is required, `null` is the only way the recursion can end.
fn allow_null_for_optional_recursion(property: &mut Value, refs: &Refs) {
    if !sole_ref(property).is_some_and(|reference| refs.is_recursive(reference)) {
        return;
    }
    let Value::Object(schema) = property else {
        return;
    };
    // Annotations describe the property, so they stay outside, the way
    // Pydantic writes an `Optional` model.
    let mut nullable: Map<String, Value> = schema
        .iter()
        .filter(|(key, _)| ANNOTATIONS.contains(&key.as_str()))
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect();
    schema.retain(|key, _| !ANNOTATIONS.contains(&key.as_str()));
    let reference = Value::Object(std::mem::take(schema));
    nullable.insert("anyOf".to_string(), json!([reference, { "type": "null" }]));
    *schema = nullable;
}

/// The `$ref` a schema comes down to: its own, or the one inside a one-entry
/// `allOf` or `anyOf`.
fn sole_ref(schema: &Value) -> Option<&str> {
    if let Some(Value::String(reference)) = schema.get("$ref") {
        return Some(reference);
    }
    ["allOf", "anyOf"]
        .into_iter()
        .find_map(|keyword| match schema.get(keyword) {
            Some(Value::Array(entries)) if entries.len() == 1 => sole_ref(&entries[0]),
            _ => None,
        })
}

/// Turns a one-entry `allOf` (older Pydantic wraps a `$ref` in one to give it
/// a description), or a `$ref` with sibling keywords, into a one-entry
/// `anyOf`. The siblings stay where they are, so every constraint still
/// applies.
fn wrap_in_any_of(schema: &mut Map<String, Value>, path: &str, refs: &Refs) {
    if schema.contains_key("anyOf") {
        return;
    }
    if matches!(schema.get("allOf"), Some(Value::Array(entries)) if entries.len() == 1)
        && !refs.point_into(&format!("{path}/allOf"))
    {
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
    fn all_of_that_a_ref_points_inside_is_left_alone() {
        let schema = json!({
            "type": "object",
            "properties": {
                "a": {"allOf": [{"type": "string"}]},
                "b": {"$ref": "#/properties/a/allOf/0"}
            },
            "required": ["a", "b"]
        });

        let strict = to_strict_json_schema(&schema);

        assert_eq!(strict["properties"]["a"], schema["properties"]["a"]);
        assert_eq!(
            strict.pointer("/properties/a/allOf/0"),
            Some(&json!({"type": "string"}))
        );
        assert_eq!(
            strict["properties"]["b"],
            json!({"$ref": "#/properties/a/allOf/0"})
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
    fn a_property_that_a_ref_points_at_keeps_its_type() {
        // zod-to-json-schema refers back to a reused schema this way.
        let schema = json!({
            "type": "object",
            "properties": {
                "a": {"type": "string"},
                "b": {"$ref": "#/properties/a"},
                "c": {"type": "string"}
            },
            "required": ["b"]
        });

        let properties = to_strict_json_schema(&schema)["properties"].clone();

        assert_eq!(properties["a"], json!({"type": "string"}));
        assert_eq!(properties["b"], json!({"$ref": "#/properties/a"}));
        assert_eq!(properties["c"], json!({"type": ["string", "null"]}));
    }

    #[test]
    fn an_optional_recursive_reference_can_end_with_null() {
        let schema = json!({
            "type": "object",
            "properties": {"name": {"type": "string"}, "child": {"$ref": "#"}},
            "required": ["name"]
        });

        let strict = to_strict_json_schema(&schema);

        assert_eq!(strict["required"], json!(["child", "name"]));
        assert_eq!(
            strict["properties"]["child"],
            json!({"anyOf": [{"$ref": "#"}, {"type": "null"}]})
        );

        // Two definitions that refer to each other.
        let schema = json!({
            "type": "object",
            "properties": {"a": {"$ref": "#/$defs/A"}},
            "required": ["a"],
            "$defs": {
                "A": {"type": "object", "properties": {"b": {"$ref": "#/$defs/B"}}},
                "B": {"type": "object", "properties": {"a": {"$ref": "#/$defs/A"}}}
            }
        });

        let strict = to_strict_json_schema(&schema);

        assert_eq!(strict["properties"]["a"], json!({"$ref": "#/$defs/A"}));
        assert_eq!(
            strict["$defs"]["A"]["properties"]["b"],
            json!({"anyOf": [{"$ref": "#/$defs/B"}, {"type": "null"}]})
        );
        assert_eq!(
            strict["$defs"]["B"]["properties"]["a"],
            json!({"anyOf": [{"$ref": "#/$defs/A"}, {"type": "null"}]})
        );
    }

    #[test]
    fn an_optional_recursive_reference_inside_a_wrapper_can_end_with_null() {
        // Older Pydantic wraps a described reference in a one-entry `allOf`.
        let schema = json!({
            "type": "object",
            "properties": {
                "name": {"type": "string"},
                "child": {"allOf": [{"$ref": "#"}], "description": "The next one"}
            },
            "required": ["name"]
        });

        let strict = to_strict_json_schema(&schema);
        let child = &strict["properties"]["child"];

        assert_eq!(strict["required"], json!(["child", "name"]));
        assert_eq!(child["description"], "The next one");
        assert_eq!(child["anyOf"][0], json!({"anyOf": [{"$ref": "#"}]}));
        assert_eq!(child["anyOf"][1], json!({"type": "null"}));
    }

    #[test]
    fn an_optional_enum_of_objects_keeps_its_type() {
        let schema = json!({
            "type": "object",
            "properties": {
                "plan": {"enum": [{"kind": "fixed"}]},
                "fixed": {"const": {"kind": "fixed"}},
                "tier": {"const": "gold"}
            }
        });

        let properties = to_strict_json_schema(&schema)["properties"].clone();

        assert_eq!(properties["plan"], schema["properties"]["plan"]);
        assert_eq!(properties["fixed"], schema["properties"]["fixed"]);
        assert_eq!(
            properties["tier"],
            json!({"anyOf": [{"const": "gold"}, {"type": "null"}]})
        );
    }

    #[test]
    fn an_optional_reference_to_a_plain_definition_keeps_its_type() {
        // Pydantic's `Field(default_factory=City)` has no `default`, and its
        // model rejects `None`.
        let schema = json!({
            "type": "object",
            "properties": {"home": {"$ref": "#/$defs/City"}},
            "$defs": {
                "City": {
                    "type": "object",
                    "properties": {"name": {"type": "string"}},
                    "required": ["name"]
                }
            }
        });

        let strict = to_strict_json_schema(&schema);

        assert_eq!(
            strict["properties"]["home"],
            json!({"$ref": "#/$defs/City"})
        );
        assert_eq!(strict["required"], json!(["home"]));
    }

    #[test]
    fn required_names_without_a_property_are_left_alone() {
        let schema = json!({
            "type": "object",
            "properties": {
                "name": {"type": "string"},
                "home": {"type": "object", "properties": {"city": {"type": "string"}}}
            },
            "required": ["name", "id"]
        });

        let strict = to_strict_json_schema(&schema);

        assert_eq!(strict["required"], json!(["name", "id"]));
        assert!(strict.get("additionalProperties").is_none());
        assert_eq!(strict["properties"]["name"], json!({"type": "string"}));
        // Nested objects are still rewritten.
        assert_eq!(strict["properties"]["home"]["additionalProperties"], false);
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
        // Optional and recursive, so it may also end with `null`.
        assert_eq!(
            strict["$defs"]["L11"]["properties"]["self"],
            json!({"anyOf": [{"$ref": "#/$defs/L11"}, {"type": "null"}], "description": "itself"})
        );
    }

    #[test]
    fn strict_tools_get_the_same_rewrite() {
        let parameters = json!({
            "type": "object",
            "properties": {
                "location": {"type": "string"},
                "units": {"type": "string", "enum": ["celsius", "fahrenheit"]}
            },
            "required": ["location"]
        });
        let mut tool = ToolDefinition::new("get_weather");
        tool.parameters = Some(parameters.clone());

        tool.strict = Some(true);
        let sent = tool_parameters(&tool).unwrap();
        assert_eq!(sent["required"], json!(["location", "units"]));
        assert_eq!(sent["additionalProperties"], false);
        assert_eq!(
            sent["properties"]["units"],
            json!({"anyOf": [{"type": "string", "enum": ["celsius", "fahrenheit"]}, {"type": "null"}]})
        );

        for strict in [None, Some(false)] {
            tool.strict = strict;
            assert_eq!(tool_parameters(&tool), Some(parameters.clone()));
        }
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
