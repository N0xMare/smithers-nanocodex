use std::fs;
use std::path::{Path, PathBuf};

use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use smithers_nanocodex::{
    Capabilities,
    capabilities::{
        BRIDGE_PROTOCOL_NAME, BRIDGE_PROTOCOL_VERSION, CHECKPOINT_CODEC, CHECKPOINT_CODEC_VERSION,
        NANOCODEX_VERSION, SHIPPED_TARGETS, SNAPSHOT_VERSION, TOOL_PROFILE,
    },
};

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

fn shipped_targets_from_schema(schema: &Value) -> Vec<String> {
    schema["properties"]["target"]["enum"]
        .as_array()
        .expect("capability target enum")
        .iter()
        .map(|value| {
            value
                .as_str()
                .expect("capability target enum entry")
                .to_owned()
        })
        .collect()
}

fn capabilities_for_schema_validation(
    mut capabilities: Value,
    shipped_targets: &[String],
) -> Value {
    let target = capabilities["target"]
        .as_str()
        .expect("runtime capabilities include a target");
    assert!(
        !target.is_empty(),
        "runtime target must be a non-empty string"
    );
    if !shipped_targets.iter().any(|shipped| shipped == target) {
        capabilities["target"] = json!(shipped_targets[0]);
    }
    capabilities
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

    let runtime =
        serde_json::to_value(Capabilities::current()).expect("serialize runtime capabilities");
    let shipped = shipped_targets_from_schema(&schemas["capabilities-v1.schema.json"]);
    assert_eq!(
        shipped,
        SHIPPED_TARGETS
            .iter()
            .map(|target| (*target).to_owned())
            .collect::<Vec<_>>()
    );
    assert!(
        shipped
            .iter()
            .any(|target| runtime["target"].as_str() == Some(target)),
        "runtime target must be a shipped 0.0.2 triple: {}",
        runtime["target"]
    );
    validator("capabilities-v1.schema.json")
        .validate(&runtime)
        .expect("published capabilities must satisfy their schema");
}

#[test]
fn runtime_target_is_the_compiled_shipped_triple() {
    let capabilities = Capabilities::current();
    assert_eq!(capabilities.target, env!("SMITHERS_NANOCODEX_TARGET"));
    assert!(
        SHIPPED_TARGETS.contains(&capabilities.target),
        "runtime target {} is not a shipped 0.0.2 triple",
        capabilities.target
    );
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
    let runtime = serde_json::to_value(Capabilities::current()).expect("serialize capabilities");
    // Shape fixtures pin one shipped Linux target. Other CI hosts still
    // verify every platform-independent capability field.
    let runtime = capabilities_for_schema_validation(
        runtime,
        &[hello["data"]["target"]
            .as_str()
            .expect("hello fixture target")
            .to_owned()],
    );
    assert_eq!(hello["data"], runtime);
}

#[test]
fn capability_contract_normalization_changes_only_the_build_target() {
    let mut runtime =
        serde_json::to_value(Capabilities::current()).expect("serialize capabilities");
    runtime["target"] = json!("powerpc64le-unknown-linux-gnu");

    let normalized = capabilities_for_schema_validation(
        runtime.clone(),
        &[
            "x86_64-unknown-linux-gnu".to_owned(),
            "aarch64-apple-darwin".to_owned(),
        ],
    );

    assert_eq!(normalized["target"], "x86_64-unknown-linux-gnu");
    runtime["target"] = normalized["target"].clone();
    assert_eq!(normalized, runtime);
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
fn historical_v0_0_1_pin_remains_immutable() {
    const VERSION: &str = "0.0.1";
    const TARGET: &str = "x86_64-unknown-linux-gnu";
    const TAG_COMMIT: &str = "56d8b4fd54bf14e9f2874e5a010b8e301f8f695b";
    const ARCHIVE_SHA256: &str = "0e14425b3e0af5c3b1663b4db2a15302cbaa7c03e917babd841ae7fde2a1ab73";

    let baseline = parse_json(
        &repository_root()
            .join("docs")
            .join("releases")
            .join("v0.0.1.json"),
    );
    let archive = format!("smithers-nanocodex-v{VERSION}-{TARGET}.tar.gz");

    assert_eq!(baseline["schemaVersion"], 1);
    assert!(baseline.get("toolProfile").is_none());
    assert!(baseline.get("artifacts").is_none());
    assert!(baseline["artifact"].is_object());
    assert!(baseline["contract"].get("toolProfile").is_none());
    assert_eq!(
        baseline["baselineId"],
        format!("smithers-nanocodex/v{VERSION}/{TARGET}")
    );
    assert_eq!(baseline["status"], "qualified");
    assert_eq!(baseline["release"]["version"], VERSION);
    assert_eq!(baseline["release"]["tag"], format!("v{VERSION}"));
    assert_eq!(baseline["release"]["tagCommit"], TAG_COMMIT);
    assert_eq!(baseline["release"]["publishedAt"], "2026-07-29T23:57:32Z");
    assert_eq!(baseline["artifact"]["target"], TARGET);
    assert_eq!(baseline["artifact"]["fileName"], archive);
    assert_eq!(baseline["artifact"]["sha256"], ARCHIVE_SHA256);
    assert_eq!(baseline["artifact"]["sizeBytes"], 6_286_271);
    assert_eq!(
        baseline["artifact"]["downloadUrl"],
        format!(
            "https://github.com/N0xMare/smithers-nanocodex/releases/download/v{VERSION}/{archive}"
        )
    );
    assert_eq!(baseline["artifact"]["minimumGlibcVersion"], "2.35");
    assert_eq!(
        baseline["release"]["url"],
        "https://github.com/N0xMare/smithers-nanocodex/releases/tag/v0.0.1"
    );
    assert_eq!(baseline["contract"]["bridgeVersion"], VERSION);
    assert_eq!(baseline["contract"]["nanocodexVersion"], "0.3.0");
    assert_eq!(baseline["contract"]["protocol"], "smithers.nanocodex/1");
    assert_eq!(
        baseline["contract"]["checkpointCodec"],
        "nanocodex.session-snapshot/1"
    );
    assert_eq!(baseline["contract"]["snapshotVersion"], 1);
    assert_eq!(
        baseline["contract"]["policyFingerprint"],
        "smithers.nanocodex.policy-fingerprint/1"
    );
    assert_eq!(baseline["qualification"]["date"], "2026-07-29");
    assert_eq!(baseline["qualification"]["bridgeArtifact"], true);
    assert_eq!(baseline["qualification"]["smithersAdapter"], false);
    let checks = baseline["qualification"]["checks"]
        .as_array()
        .expect("historical qualification checks");
    assert!(
        checks
            .iter()
            .any(|check| check.as_str() == Some("exact Bubblewrap PID-containment profile")),
        "historical pin must keep its Bubblewrap qualification line"
    );

    let pin_bytes = fs::read(
        repository_root()
            .join("docs")
            .join("releases")
            .join("v0.0.1.json"),
    )
    .expect("read historical pin");
    assert_eq!(
        lowercase_hex(&Sha256::digest(pin_bytes)),
        "9f87c17faf28aa14763dfb7a8cd45c7807d09d902a86a353fe8348fc04e2907d"
    );
}

#[test]
fn current_release_baseline_matches_the_package_contract() {
    const VERSION: &str = "0.0.2";

    let baseline = parse_json(
        &repository_root()
            .join("docs")
            .join("releases")
            .join("v0.0.2.json"),
    );
    assert_eq!(baseline["schemaVersion"], 2);
    assert_eq!(
        baseline["baselineId"],
        format!("smithers-nanocodex/v{VERSION}")
    );
    assert_eq!(baseline["status"], "prepared");
    assert_eq!(baseline["release"]["version"], env!("CARGO_PKG_VERSION"));
    assert_eq!(baseline["release"]["version"], VERSION);
    assert_eq!(baseline["release"]["tag"], format!("v{VERSION}"));
    assert_eq!(
        baseline["release"]["url"],
        format!("https://github.com/N0xMare/smithers-nanocodex/releases/tag/v{VERSION}")
    );

    let artifacts = baseline["artifacts"]
        .as_array()
        .expect("v0.0.2 lists shipped artifacts");
    let targets = artifacts
        .iter()
        .map(|artifact| artifact["target"].as_str().expect("artifact target"))
        .collect::<Vec<_>>();
    assert_eq!(targets, SHIPPED_TARGETS);
    for artifact in artifacts {
        let target = artifact["target"].as_str().expect("artifact target");
        let archive = format!("smithers-nanocodex-v{VERSION}-{target}.tar.gz");
        assert_eq!(artifact["fileName"], archive);
        assert_eq!(
            artifact["downloadUrl"],
            format!(
                "https://github.com/N0xMare/smithers-nanocodex/releases/download/v{VERSION}/{archive}"
            )
        );
        assert!(artifact.get("sha256").is_none());
        assert!(artifact.get("sizeBytes").is_none());
    }
    assert_eq!(baseline["artifacts"][0]["minimumGlibcVersion"], "2.35");
    assert_eq!(baseline["artifacts"][1]["minimumMacosVersion"], "15");
    assert!(
        baseline["artifacts"][1]
            .get("minimumGlibcVersion")
            .is_none()
    );
    assert!(
        baseline["artifacts"][0]
            .get("minimumMacosVersion")
            .is_none()
    );

    assert_eq!(baseline["contract"]["bridgeVersion"], VERSION);
    assert_eq!(baseline["contract"]["nanocodexVersion"], NANOCODEX_VERSION);
    assert_eq!(baseline["contract"]["toolProfile"], TOOL_PROFILE);
    assert_eq!(
        baseline["contract"]["protocol"],
        format!("{BRIDGE_PROTOCOL_NAME}/{BRIDGE_PROTOCOL_VERSION}")
    );
    assert_eq!(
        baseline["contract"]["checkpointCodec"],
        format!("{CHECKPOINT_CODEC}/{CHECKPOINT_CODEC_VERSION}")
    );
    assert_eq!(baseline["contract"]["snapshotVersion"], SNAPSHOT_VERSION);
    let policy_fixture = parse_json(
        &repository_root()
            .join("docs")
            .join("fixtures")
            .join("policy-fingerprint-v1.json"),
    );
    assert_eq!(
        baseline["contract"]["policyFingerprint"],
        policy_fixture["algorithm"]
    );
    assert_eq!(baseline["qualification"]["bridgeArtifact"], false);
    assert_eq!(baseline["qualification"]["smithersAdapter"], false);
    let checks = baseline["qualification"]["checks"]
        .as_array()
        .expect("prepared pin lists qualification checks");
    assert!(checks.iter().any(|check| check.as_str()
        == Some("reject 0.0.1 / Nanocodex 0.3.0 / nanocodex-stock-0.3.0 envelopes before spawn")));
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
            "{{\"fingerprintVersion\":1,\"instructions\":{encoded_instructions},\"tools\":{{\"profile\":\"{TOOL_PROFILE}\",\"codeMode\":true,\"mcp\":false,\"subagents\":false}}}}"
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

    let construction_checks = fixture["constructionChecks"]
        .as_array()
        .expect("constructionChecks array");
    assert!(
        !construction_checks.is_empty(),
        "constructionChecks must execute at least one harness case"
    );
    for name in ["control-escapes", "other-c0", "unescaped-slash"] {
        assert!(
            fixture["vectors"]
                .as_array()
                .expect("vectors array")
                .iter()
                .any(|vector| vector["name"].as_str() == Some(name)),
            "missing J(s) vector {name}"
        );
    }
    for check in construction_checks {
        let name = check["name"].as_str().expect("construction check name");
        let raw = check["rawPolicyJson"]
            .as_str()
            .expect("construction check rawPolicyJson");
        let expected_vector = check["normalizedCanonicalVector"]
            .as_str()
            .expect("construction check vector name");
        let canonical = normalize_raw_policy(raw);
        let vector = fixture["vectors"]
            .as_array()
            .expect("vectors array")
            .iter()
            .find(|vector| vector["name"].as_str() == Some(expected_vector))
            .unwrap_or_else(|| panic!("construction check {name} names unknown vector"));
        assert_eq!(canonical, vector["canonicalUtf8"], "construction {name}");
        let digest = Sha256::digest(canonical.as_bytes());
        assert_eq!(
            format!("sha256:{}", lowercase_hex(&digest)),
            check["fingerprint"],
            "construction fingerprint {name}"
        );
        assert_eq!(check["fingerprint"], vector["fingerprint"]);
    }
}

fn normalize_raw_policy(raw: &str) -> String {
    let value: Value = serde_json::from_str(raw).expect("construction check JSON");
    let object = value.as_object().expect("construction check object");
    assert_eq!(
        object.len(),
        3,
        "construction check field set must be exact"
    );
    assert!(object.contains_key("fingerprintVersion"));
    assert!(object.contains_key("instructions"));
    assert!(object.contains_key("tools"));
    assert_eq!(
        object["fingerprintVersion"]
            .as_f64()
            .expect("fingerprintVersion number"),
        1.0
    );
    let tools = object["tools"]
        .as_object()
        .expect("construction check tools object");
    assert_eq!(tools.len(), 4, "tools field set must be exact");
    assert_eq!(tools["profile"], TOOL_PROFILE);
    assert_eq!(tools["codeMode"], true);
    assert_eq!(tools["mcp"], false);
    assert_eq!(tools["subagents"], false);
    let encoded_instructions =
        serde_json::to_string(&object["instructions"]).expect("serialize instructions");
    format!(
        "{{\"fingerprintVersion\":1,\"instructions\":{encoded_instructions},\"tools\":{{\"profile\":\"{TOOL_PROFILE}\",\"codeMode\":true,\"mcp\":false,\"subagents\":false}}}}"
    )
}

#[test]
fn historical_checkpoint_envelope_is_rejected_by_the_current_schema() {
    let root = repository_root();
    let schema = parse_json(&root.join("docs/schema/checkpoint-v1.schema.json"));
    jsonschema::draft202012::meta::validate(&schema).expect("checkpoint schema is valid");
    let validator = jsonschema::draft202012::options()
        .build(&schema)
        .expect("compile checkpoint schema");

    let current = parse_json(&root.join("docs/fixtures/checkpoint-v1.json"));
    validator
        .validate(&current)
        .expect("current checkpoint fixture must remain valid");

    let rejected = parse_json(&root.join("docs/fixtures/checkpoint-v0.0.1-rejected.json"));
    assert_eq!(rejected["payload"]["nanocodexVersion"], "0.3.0");
    assert_eq!(rejected["payload"]["snapshotVersion"], 1);
    assert_eq!(rejected["codec"], "nanocodex.session-snapshot");
    assert!(
        validator.validate(&rejected).is_err(),
        "0.0.1 / Nanocodex 0.3.0 envelopes must fail checkpoint-v1.schema.json"
    );
}
