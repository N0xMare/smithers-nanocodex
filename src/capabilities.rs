use serde::Serialize;

use crate::strict_json::{
    MAX_JSON_ARRAY_ELEMENTS, MAX_JSON_DEPTH, MAX_JSON_KEY_BYTES, MAX_JSON_NODES,
    MAX_JSON_OBJECT_MEMBERS, MAX_JSON_STRING_BYTES,
};

pub const BRIDGE_PROTOCOL_NAME: &str = "smithers.nanocodex";
pub const BRIDGE_PROTOCOL_VERSION: u16 = 1;
pub const NANOCODEX_VERSION: &str = "0.5.0";
pub const TOOL_PROFILE: &str = "nanocodex-stock-0.5.0";
pub const CHECKPOINT_CODEC: &str = "nanocodex.session-snapshot";
pub const CHECKPOINT_CODEC_VERSION: u16 = 1;
pub const SNAPSHOT_VERSION: u32 = 1;
pub const SHIPPED_TARGETS: &[&str] = &["x86_64-unknown-linux-gnu", "aarch64-apple-darwin"];

pub const MAX_INPUT_RECORD_BYTES: usize = 24 * 1024 * 1024;
pub const MAX_OUTPUT_RECORD_BYTES: usize = 40 * 1024 * 1024;
pub const MAX_PROMPT_BYTES: usize = 4 * 1024 * 1024;
// Leave at least 1 MiB of headroom for the Smithers checkpoint envelope under
// its absolute 16 MiB durability ceiling.
pub const MAX_SNAPSHOT_BYTES: usize = 15 * 1024 * 1024;
pub const MAX_EVENT_BYTES: usize = 1024 * 1024;
pub const MAX_EVENT_TOTAL_BYTES: usize = 16 * 1024 * 1024;
pub const MAX_STDERR_BYTES: usize = 64 * 1024;
pub const MAX_COMMAND_RECORDS: usize = 256;
pub const MAX_MANAGED_AUTH_FILE_BYTES: usize = 1024 * 1024;

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Capabilities {
    pub bridge_version: &'static str,
    pub target: &'static str,
    pub nanocodex_version: &'static str,
    pub protocol: ProtocolCapabilities,
    pub checkpoint: CheckpointCapabilities,
    pub authentication_modes: [&'static str; 2],
    pub transport_modes: [&'static str; 1],
    pub features: FeatureCapabilities,
    pub limits: ProtocolLimits,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ProtocolCapabilities {
    pub name: &'static str,
    pub versions: [u16; 1],
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CheckpointCapabilities {
    pub codec: &'static str,
    pub codec_versions: [u16; 1],
    pub snapshot_versions: [u32; 1],
    pub continuation_modes: [&'static str; 1],
    pub resume_requires_same_canonical_workspace: bool,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct FeatureCapabilities {
    pub code_mode: bool,
    pub code_mode_disable: bool,
    pub websocket_https_fallback: bool,
    pub custom_endpoints: bool,
    pub mcp: bool,
    pub subagents: bool,
    pub steering: bool,
    pub workspace_relocation: bool,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ProtocolLimits {
    pub max_input_record_bytes: usize,
    pub max_output_record_bytes: usize,
    pub max_prompt_bytes: usize,
    pub max_snapshot_bytes: usize,
    pub max_event_bytes: usize,
    pub max_event_total_bytes: usize,
    pub max_stderr_bytes: usize,
    pub max_command_records: usize,
    pub max_json_depth: usize,
    pub max_json_nodes: usize,
    pub max_json_object_members: usize,
    pub max_json_array_elements: usize,
    pub max_json_string_bytes: usize,
    pub max_json_key_bytes: usize,
    pub max_managed_auth_file_bytes: usize,
}

impl Capabilities {
    #[must_use]
    pub const fn current() -> Self {
        Self {
            bridge_version: env!("CARGO_PKG_VERSION"),
            target: env!("SMITHERS_NANOCODEX_TARGET"),
            nanocodex_version: NANOCODEX_VERSION,
            protocol: ProtocolCapabilities {
                name: BRIDGE_PROTOCOL_NAME,
                versions: [BRIDGE_PROTOCOL_VERSION],
            },
            checkpoint: CheckpointCapabilities {
                codec: CHECKPOINT_CODEC,
                codec_versions: [CHECKPOINT_CODEC_VERSION],
                snapshot_versions: [SNAPSHOT_VERSION],
                continuation_modes: ["resume"],
                resume_requires_same_canonical_workspace: true,
            },
            authentication_modes: ["api-key-env", "chatgpt"],
            transport_modes: ["websocket"],
            features: FeatureCapabilities {
                code_mode: true,
                code_mode_disable: false,
                websocket_https_fallback: true,
                custom_endpoints: false,
                mcp: false,
                subagents: false,
                steering: false,
                workspace_relocation: false,
            },
            limits: ProtocolLimits {
                max_input_record_bytes: MAX_INPUT_RECORD_BYTES,
                max_output_record_bytes: MAX_OUTPUT_RECORD_BYTES,
                max_prompt_bytes: MAX_PROMPT_BYTES,
                max_snapshot_bytes: MAX_SNAPSHOT_BYTES,
                max_event_bytes: MAX_EVENT_BYTES,
                max_event_total_bytes: MAX_EVENT_TOTAL_BYTES,
                max_stderr_bytes: MAX_STDERR_BYTES,
                max_command_records: MAX_COMMAND_RECORDS,
                max_json_depth: MAX_JSON_DEPTH,
                max_json_nodes: MAX_JSON_NODES,
                max_json_object_members: MAX_JSON_OBJECT_MEMBERS,
                max_json_array_elements: MAX_JSON_ARRAY_ELEMENTS,
                max_json_string_bytes: MAX_JSON_STRING_BYTES,
                max_json_key_bytes: MAX_JSON_KEY_BYTES,
                max_managed_auth_file_bytes: MAX_MANAGED_AUTH_FILE_BYTES,
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn capabilities_are_explicit_and_resume_only() {
        let capabilities = Capabilities::current();
        assert_eq!(capabilities.protocol.versions, [1]);
        assert_eq!(capabilities.nanocodex_version, NANOCODEX_VERSION);
        assert_eq!(TOOL_PROFILE, "nanocodex-stock-0.5.0");
        assert_eq!(
            SHIPPED_TARGETS,
            &["x86_64-unknown-linux-gnu", "aarch64-apple-darwin"]
        );
        assert_eq!(capabilities.target, env!("SMITHERS_NANOCODEX_TARGET"));
        assert!(
            SHIPPED_TARGETS.contains(&capabilities.target),
            "runtime target {} is not a shipped 0.0.2 triple",
            capabilities.target
        );
        assert_eq!(capabilities.checkpoint.continuation_modes, ["resume"]);
        assert!(capabilities.features.code_mode);
        assert!(capabilities.features.websocket_https_fallback);
        assert!(!capabilities.features.custom_endpoints);
        assert!(!capabilities.features.workspace_relocation);
        assert!(!capabilities.features.subagents);
    }

    #[test]
    fn capabilities_are_strict_json_values() {
        let value = serde_json::to_value(Capabilities::current()).unwrap();
        assert_eq!(value["protocol"]["name"], BRIDGE_PROTOCOL_NAME);
        assert_eq!(value["nanocodexVersion"], NANOCODEX_VERSION);
        assert_eq!(value["limits"]["maxJsonDepth"], MAX_JSON_DEPTH);
        assert_eq!(
            value["limits"]["maxManagedAuthFileBytes"],
            MAX_MANAGED_AUTH_FILE_BYTES
        );
    }
}
