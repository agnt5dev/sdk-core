# AGNT5 deterministic eval scorers

SDK-core owns this worker-independent library. SDK native bridges and the native
runtime call the same implementation; the pure-Go SDK conforms to its JSON fixtures.

`structured_assertions(envelope)` accepts `input`, `output`, `expected`, and a
`config` object containing 1–64 assertions (`{name?, expr}`) and an optional
`score_threshold` in [0,1], default 1. Scores are the fraction of passing assertions.
`metadata.assertions` preserves each assertion's name, expression, and pass status.
Invalid configuration produces `config_error`; missing nested fields, invalid JSON,
wrong operand types, or exhausted evaluation limits produce `input_error`. Both
errors always fail, including when the configured score threshold is zero.

Supported expressions:

- JSON string, number, boolean, and null literals; parentheses; `!`, `&&`, `||`.
- `==`, `!=`, `<`, `<=`, `>`, `>=`. Ordering requires safe finite numbers;
  equality is structural and treats numeric 1 and 1.0 as equal. Numeric comparisons
  are limited to magnitude 9,007,199,254,740,991 for cross-language consistency.
- `input`, `output`, `expected` and dot-separated object fields. Missing roots are
  null; missing nested fields are errors. `_json` aliases parse JSON-encoded strings.
- `is_array`, `is_object`, `is_string`, `is_number`, `is_boolean`, `is_null`.
- `size` (array/object length or Unicode scalar count), `unique` (structural array
  uniqueness), and `all(array, predicate)` / `any(array, predicate)` using one of
  the type predicates above. Empty `all` is true; empty `any` is false.

No host code, arbitrary functions, indexing, arithmetic, regular expressions, or
network calls are evaluated. Expressions are limited to 4,096 bytes, 32 levels
of nesting, and 64 binary operators. Assertion names must be unique and at most
256 bytes. Input is limited to 1 MiB, depth 64, and 100,000 JSON nodes. Array
functions accept at most 4,096 elements; a shared 100,000-step budget also bounds
structural comparisons across all assertions. Boolean evaluation short circuits,
but all assertion expressions are parsed before any assertion executes.

Example: `size(output_json) == expected.expected_length`.

Python and TypeScript local helpers require their matching native extension.
TypeScript edge clients can submit scorer recipes for runtime execution; they do
not execute this native scorer locally. Release automation publishes the shared crate before SDK-core, then dependent
native SDK packages can use the registry version.

`validate_online_config` parses every expression before an online policy is
activated. Only input/output evidence is available online; reference-answer roots
and expected-field bindings are rejected, including in short-circuited branches.
