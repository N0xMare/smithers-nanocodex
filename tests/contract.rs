use std::fs;
use std::path::{Path, PathBuf};

use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use smithers_nanocodex::Capabilities;

fn repository_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

fn parse_json(path: &Path) -> Value {
    let bytes = fs::read(path).unwrap_or_else(|error| panic!("read {}: {error}", path.display()));
    serde_json::from_slice(&bytes)
        .unwrap_or_else(|error| panic!("parse {} as JSON: {error}", path.display()))
}

fn parse_jsonl(path: &Path) -> Vec<Value> {
    let text =
        fs::read_to_string(path).unwrap_or_else(|error| panic!("read {}: {error}", path.display()));
    text.lines()
        .enumerate()
        .filter(|(_, line)| !line.trim().is_empty())
        .map(|(index, line)| {
            serde_json::from_str(line).unwrap_or_else(|error| {
                panic!(
                    "parse {} line {} as JSON: {error}",
                    path.display(),
                    index + 1
                )
            })
        })
        .collect()
}

fn lowercase_hex(bytes: &[u8]) -> String {
    use std::fmt::Write;

    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        write!(output, "{byte:02x}").expect("write to String");
    }
    output
}

#[test]
fn schemas_and_protocol_fixtures_are_valid_json() {
    let root = repository_root();
    let schema_dir = root.join("docs/schema");
    let fixture_dir = root.join("docs/fixtures");
    let schema_names = [
        "capabilities-v1.schema.json",
        "checkpoint-v1.schema.json",
        "client-v1.schema.json",
        "server-v1.schema.json",
    ];
    let schemas = schema_names
        .iter()
        .map(|name| (*name, parse_json(&schema_dir.join(name))))
        .collect::<std::collections::BTreeMap<_, _>>();

    let mut registry = jsonschema::Registry::new();
    for (name, schema) in &schemas {
        assert_eq!(
            schema["$schema"],
            "https://json-schema.org/draft/2020-12/schema"
        );
        assert!(
            schema["additionalProperties"] == false || schema["oneOf"].is_array(),
            "{name} must be a strict object schema or an explicit record union"
        );
        jsonschema::draft202012::meta::validate(schema)
            .unwrap_or_else(|error| panic!("{name} is not a valid Draft 2020-12 schema: {error}"));
        registry = registry
            .add(
                schema["$id"].as_str().expect("schema must declare $id"),
                schema.clone(),
            )
            .unwrap_or_else(|error| panic!("register {name}: {error}"));
    }
    let registry = registry.prepare().expect("prepare schema registry");
    let validator = |name: &str| {
        jsonschema::draft202012::options()
            .with_registry(&registry)
            .build(&schemas[name])
            .unwrap_or_else(|error| panic!("compile {name}: {error}"))
    };

    let checkpoint = parse_json(&fixture_dir.join("checkpoint-v1.json"));
    validator("checkpoint-v1.schema.json")
        .validate(&checkpoint)
        .unwrap_or_else(|error| panic!("checkpoint-v1.json violates its schema: {error}"));

    let policy = parse_json(&fixture_dir.join("policy-fingerprint-v1.json"));
    assert!(
        policy.is_object(),
        "policy-fingerprint-v1.json must contain one JSON object"
    );

    for name in ["client-cancel-v1.jsonl", "client-success-v1.jsonl"] {
        let records = parse_jsonl(&fixture_dir.join(name));
        assert!(
            !records.is_empty(),
            "{name} must contain at least one record"
        );
        for (index, record) in records.iter().enumerate() {
            validator("client-v1.schema.json")
                .validate(record)
                .unwrap_or_else(|error| {
                    panic!("{name} line {} violates its schema: {error}", index + 1)
                });
        }
    }

    let server_name = "server-success-v1.jsonl";
    let records = parse_jsonl(&fixture_dir.join(server_name));
    assert!(
        !records.is_empty(),
        "{server_name} must contain at least one record"
    );
    for (index, record) in records.iter().enumerate() {
        validator("server-v1.schema.json")
            .validate(record)
            .unwrap_or_else(|error| {
                panic!(
                    "{server_name} line {} violates its schema: {error}",
                    index + 1
                )
            });
    }

    let capabilities =
        serde_json::to_value(Capabilities::current()).expect("serialize runtime capabilities");
    validator("capabilities-v1.schema.json")
        .validate(&capabilities)
        .expect("runtime capabilities must satisfy their schema");
}

#[test]
fn golden_hello_capabilities_match_the_implementation() {
    let records = parse_jsonl(
        &repository_root()
            .join("docs/fixtures")
            .join("server-success-v1.jsonl"),
    );
    let hello = records.first().expect("server fixture has a hello record");

    assert_eq!(hello["type"], "hello");
    let mut runtime =
        serde_json::to_value(Capabilities::current()).expect("serialize capabilities");
    if !cfg!(all(target_os = "linux", target_arch = "x86_64")) {
        // The published v1 artifact is Linux x86_64 only. Other CI hosts still
        // verify every platform-independent capability field.
        runtime["target"] = hello["data"]["target"].clone();
    }
    assert_eq!(hello["data"], runtime);
}

#[test]
fn capability_schema_constants_match_the_implementation() {
    let schema = parse_json(
        &repository_root()
            .join("docs/schema")
            .join("capabilities-v1.schema.json"),
    );
    let capabilities =
        serde_json::to_value(Capabilities::current()).expect("serialize capabilities");
    let expected = &schema["properties"]["limits"]["properties"];
    let actual = capabilities["limits"]
        .as_object()
        .expect("capability limits object");

    assert_eq!(
        expected.as_object().expect("schema limit properties").len(),
        actual.len()
    );
    for (name, value) in actual {
        assert_eq!(expected[name]["const"], *value, "limit {name}");
    }
}

#[test]
fn policy_fingerprint_vectors_reproduce_canonical_bytes_and_hashes() {
    let fixture = parse_json(
        &repository_root()
            .join("docs/fixtures")
            .join("policy-fingerprint-v1.json"),
    );
    assert_eq!(
        fixture["algorithm"],
        "smithers.nanocodex.policy-fingerprint/1"
    );

    for vector in fixture["vectors"].as_array().expect("vectors array") {
        let name = vector["name"].as_str().expect("vector name");
        let encoded_instructions =
            serde_json::to_string(&vector["instructions"]).expect("serialize vector instructions");
        let canonical = format!(
            "{{\"fingerprintVersion\":1,\"instructions\":{encoded_instructions},\"tools\":{{\"profile\":\"nanocodex-stock-0.3.0\",\"codeMode\":true,\"mcp\":false,\"subagents\":false}}}}"
        );
        assert_eq!(canonical, vector["canonicalUtf8"], "canonical {name}");
        assert_eq!(
            lowercase_hex(canonical.as_bytes()),
            vector["canonicalUtf8Hex"],
            "hex {name}"
        );

        let digest = Sha256::digest(canonical.as_bytes());
        assert_eq!(
            format!("sha256:{}", lowercase_hex(&digest)),
            vector["fingerprint"],
            "fingerprint {name}"
        );
    }

    assert_eq!(
        fixture["constructionChecks"][0]["fingerprint"],
        json!("sha256:e4580c36cd5e0b89d1bdc7aeda2c3664ca61d239fdd9807e6b62019b81dbde86")
    );
}
