use serde_json::Value;
#[test]
fn sdk_owned_assertion_contract() {
    let fixtures: Value =
        serde_json::from_str(include_str!("fixtures/structured_assertions.json")).unwrap();
    for case in fixtures["cases"].as_array().unwrap() {
        let result = agnt5_eval_scorers::structured_assertions(&case["input"]);
        assert_eq!(
            result["score"].as_f64(),
            case["expect"]["score"].as_f64(),
            "{}: {result}",
            case["name"]
        );
        for key in ["passed", "label"] {
            assert_eq!(
                result[key], case["expect"][key],
                "{}: {result}",
                case["name"]
            );
        }
        if result["label"] == "pass" || result["label"] == "fail" {
            assert_eq!(
                result["metadata"]["assertions"].as_array().unwrap().len(),
                case["input"]["config"]["assertions"]
                    .as_array()
                    .unwrap()
                    .len()
            );
        }
    }
}

#[test]
fn resource_limits_fail_closed() {
    use serde_json::json;
    let score = |expr: &str, output: Value| {
        agnt5_eval_scorers::structured_assertions(
            &json!({"output":output,"config":{"assertions":[{"expr":expr}]}}),
        )
    };
    assert_eq!(
        score("unique(output)", json!((0..4097).collect::<Vec<_>>()))["label"],
        "input_error"
    );
    assert_eq!(
        score("unique(output)", json!((0..1000).collect::<Vec<_>>()))["label"],
        "input_error"
    );
    assert_eq!(
        score("true", json!("a".repeat(1_048_577)))["label"],
        "input_error"
    );
    let mut deep = Value::Null;
    for _ in 0..66 {
        deep = json!([deep]);
    }
    assert_eq!(score("true", deep)["label"], "input_error");
}

#[test]
fn malformed_expressions_never_panic_or_fall_through() {
    use serde_json::json;
    let alphabet = [
        "(", ")", "!", ".", "x", "0", ",", "\"", "[", "é", "💡", "&&",
    ];
    let mut seed = 7u32;
    for _ in 0..1000 {
        let mut expr = String::new();
        for _ in 0..32 {
            seed = seed.wrapping_mul(1664525).wrapping_add(1013904223);
            expr.push_str(alphabet[seed as usize % alphabet.len()]);
        }
        let score = agnt5_eval_scorers::structured_assertions(
            &json!({"output":null,"config":{"assertions":[{"expr":expr}]}}),
        );
        assert_eq!(score["passed"], false);
    }
}

#[test]
fn online_validation_uses_parsed_evidence_dependencies() {
    use serde_json::json;
    for expr in [
        "is_array(output_json)",
        "size(output) > 0 && is_object(input)",
        "output.expected == 1",
        "output == \"expected_json\"",
    ] {
        assert!(
            agnt5_eval_scorers::validate_online_config(&json!({"assertions":[{"expr":expr}]}))
                .is_ok(),
            "{expr}"
        );
    }
    for expr in [
        "output == expected",
        "is_null(expected_json)",
        "true || expected.value == 1",
        "false && is_array(expected)",
        "is_array(",
        "unknown(output)",
    ] {
        assert!(
            agnt5_eval_scorers::validate_online_config(&json!({"assertions":[{"expr":expr}]}))
                .is_err(),
            "{expr}"
        );
    }
    assert!(agnt5_eval_scorers::validate_online_config(
        &json!({"assertions":[{"expr":"true"}],"expected_field":"answer"})
    )
    .is_err());
    assert!(agnt5_eval_scorers::validate_online_config(
        &json!({"assertions":[{"expr":"true"}],"score_threshold":2})
    )
    .is_err());
}
