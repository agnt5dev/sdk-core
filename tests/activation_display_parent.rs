use agnt5_sdk_core::pb::BeginActivationRequest;
use prost::Message;

// Field 15 is the additive, reader-only parent in the shared Engine contract.
// Exercise the generated wire type used by language-native activation clients.
#[test]
fn begin_activation_preserves_display_parent_on_the_wire() {
    let mut wire = BeginActivationRequest {
        project_id: "project".into(),
        run_id: "run".into(),
        parent_activation_id: "durable-step".into(),
        ..Default::default()
    }
    .encode_to_vec();
    let parent = b"agent-iteration";
    wire.extend([0x7a, parent.len() as u8]);
    wire.extend(parent);

    let decoded = BeginActivationRequest::decode(wire.as_slice()).unwrap();
    assert_eq!(decoded.parent_activation_id, "durable-step");
    assert_eq!(decoded.encode_to_vec(), wire);
}
