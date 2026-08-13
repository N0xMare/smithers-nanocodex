use std::path::PathBuf;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{
    capabilities::{BRIDGE_PROTOCOL_NAME, BRIDGE_PROTOCOL_VERSION},
    error::PublicError,
};

const IDENTIFIER_MAX_BYTES: usize = 128;
const CANCEL_REASON_MAX_BYTES: usize = 128;
const IDENTIFIER_REQUIREMENT: &str =
    "must contain 1 to 128 bytes using only ASCII letters, digits, '.', '_', ':', or '-'";

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ClientFrame {
    pub protocol: String,
    pub version: u16,
    #[serde(rename = "type")]
    pub kind: String,
    pub command_id: String,
    #[serde(default)]
    pub request_id: Option<String>,
    #[serde(default)]
    pub session_id: Option<String>,
    pub data: Value,
}

impl ClientFrame {
    pub fn validate_envelope(&self) -> Result<(), PublicError> {
        if self.protocol != BRIDGE_PROTOCOL_NAME {
            return Err(PublicError::protocol(
                "protocol_name_mismatch",
                "The command protocol name is not supported.",
            ));
        }
        if self.version != BRIDGE_PROTOCOL_VERSION {
            return Err(PublicError::protocol(
                "protocol_version_mismatch",
                "The command protocol version is not supported.",
            ));
        }
        if !valid_id(&self.command_id) {
            return Err(PublicError::protocol(
                "invalid_command_id",
                format!("commandId {IDENTIFIER_REQUIREMENT}."),
            ));
        }
        Ok(())
    }

    pub fn into_start(self) -> Result<TurnStart, PublicError> {
        if self.kind != "turn.start" {
            return Err(PublicError::protocol(
                "expected_turn_start",
                "The first command must be turn.start.",
            ));
        }
        let request_id = required_id(self.request_id, "requestId")?;
        if self.session_id.is_some() {
            return Err(PublicError::protocol(
                "unexpected_session_id",
                "turn.start must not supply sessionId.",
            ));
        }
        let data = serde_json::from_value(self.data).map_err(|_| {
            PublicError::protocol("invalid_turn_start", "turn.start data is invalid.")
        })?;
        Ok(TurnStart {
            command_id: self.command_id,
            request_id,
            data,
        })
    }

    pub fn into_cancel(self) -> Result<TurnCancel, PublicError> {
        if self.kind != "turn.cancel" {
            return Err(PublicError::protocol(
                "unsupported_command",
                "The command type is not supported in protocol version 1.",
            ));
        }
        let request_id = required_id(self.request_id, "requestId")?;
        let session_id = self
            .session_id
            .map(|session_id| required_id(Some(session_id), "sessionId"))
            .transpose()?;
        let data: TurnCancelData = serde_json::from_value(self.data).map_err(|_| {
            PublicError::protocol("invalid_turn_cancel", "turn.cancel data is invalid.")
        })?;
        if data.reason.as_ref().is_some_and(|reason| {
            reason.is_empty()
                || reason.len() > CANCEL_REASON_MAX_BYTES
                || reason.chars().any(char::is_control)
        }) {
            return Err(PublicError::protocol(
                "invalid_turn_cancel",
                "turn.cancel reason must encode to 1 to 128 UTF-8 bytes and must not contain Unicode control characters.",
            ));
        }
        Ok(TurnCancel {
            command_id: self.command_id,
            request_id,
            session_id,
            data,
        })
    }
}

fn required_id(value: Option<String>, field: &str) -> Result<String, PublicError> {
    let Some(value) = value else {
        return Err(PublicError::protocol(
            "missing_correlation_id",
            format!("{field} is required."),
        ));
    };
    if !valid_id(&value) {
        return Err(PublicError::protocol(
            "invalid_correlation_id",
            format!("{field} {IDENTIFIER_REQUIREMENT}."),
        ));
    }
    Ok(value)
}

fn valid_id(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= IDENTIFIER_MAX_BYTES
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b':'))
}

#[derive(Clone, Debug)]
pub struct TurnStart {
    pub command_id: String,
    pub request_id: String,
    pub data: TurnStartData,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TurnStartData {
    pub prompt: String,
    pub workspace: PathBuf,
    pub auth: AuthConfig,
    pub transport: TransportConfig,
    #[serde(default)]
    pub options: TurnOptions,
    #[serde(default)]
    pub continuation: Option<Continuation>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(tag = "mode", rename_all = "kebab-case", deny_unknown_fields)]
pub enum AuthConfig {
    ApiKeyEnv {
        #[serde(rename = "environmentVariable")]
        environment_variable: String,
    },
    Chatgpt {
        #[serde(default, rename = "authFile")]
        auth_file: Option<PathBuf>,
    },
}

#[derive(Clone, Debug)]
pub enum TransportConfig {
    Websocket,
}

impl<'de> Deserialize<'de> for TransportConfig {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let wire = TransportConfigWire::deserialize(deserializer)?;
        match wire.kind {
            TransportKind::Websocket => Ok(Self::Websocket),
        }
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct TransportConfigWire {
    kind: TransportKind,
}

#[derive(Deserialize)]
#[serde(rename_all = "kebab-case")]
enum TransportKind {
    Websocket,
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TurnOptions {
    #[serde(default)]
    pub instructions: Option<String>,
    #[serde(default)]
    pub thinking: Option<ThinkingLevel>,
    #[serde(default)]
    pub reasoning_mode: Option<ReasoningMode>,
    #[serde(default)]
    pub fast_mode: Option<bool>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ThinkingLevel {
    None,
    Low,
    Medium,
    High,
    Xhigh,
    Max,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ReasoningMode {
    Standard,
    Pro,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(tag = "mode", rename_all = "lowercase", deny_unknown_fields)]
pub enum Continuation {
    Resume { snapshot: Value },
}

#[derive(Clone, Debug)]
pub struct TurnCancel {
    pub command_id: String,
    pub request_id: String,
    pub session_id: Option<String>,
    pub data: TurnCancelData,
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TurnCancelData {
    #[serde(default)]
    pub reason: Option<String>,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ServerFrame {
    pub protocol: &'static str,
    pub version: u16,
    #[serde(rename = "type")]
    pub kind: String,
    pub seq: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub request_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub command_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub session_id: Option<String>,
    pub data: Value,
}

impl ServerFrame {
    #[must_use]
    pub fn new(kind: impl Into<String>, seq: u64, data: Value) -> Self {
        Self {
            protocol: BRIDGE_PROTOCOL_NAME,
            version: BRIDGE_PROTOCOL_VERSION,
            kind: kind.into(),
            seq,
            request_id: None,
            command_id: None,
            session_id: None,
            data,
        }
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;
    use crate::strict_json;

    fn start_frame() -> Value {
        json!({
            "protocol": BRIDGE_PROTOCOL_NAME,
            "version": 1,
            "type": "turn.start",
            "commandId": "command-1",
            "requestId": "request-1",
            "data": {
                "prompt": "hello",
                "workspace": "/tmp",
                "auth": {"mode": "api-key-env", "environmentVariable": "OPENAI_API_KEY"},
                "transport": {"kind": "websocket"},
                "options": {},
                "continuation": null
            }
        })
    }

    #[test]
    fn parses_and_validates_start() {
        let bytes = serde_json::to_vec(&start_frame()).unwrap();
        let frame: ClientFrame = strict_json::from_slice(&bytes).unwrap();
        frame.validate_envelope().unwrap();
        let start = frame.into_start().unwrap();
        assert_eq!(start.request_id, "request-1");
        assert_eq!(start.data.prompt, "hello");
    }

    #[test]
    fn unknown_start_data_is_rejected() {
        let mut frame = start_frame();
        frame["data"]["unexpected"] = Value::Bool(true);
        let parsed: ClientFrame =
            strict_json::from_slice(&serde_json::to_vec(&frame).unwrap()).unwrap();
        assert_eq!(parsed.into_start().unwrap_err().code, "invalid_turn_start");
    }

    #[test]
    fn transport_selection_is_mandatory() {
        let mut frame = start_frame();
        frame["data"].as_object_mut().unwrap().remove("transport");
        let parsed: ClientFrame =
            strict_json::from_slice(&serde_json::to_vec(&frame).unwrap()).unwrap();
        assert_eq!(parsed.into_start().unwrap_err().code, "invalid_turn_start");
    }

    #[test]
    fn websocket_transport_rejects_http_only_and_unknown_fields() {
        for (field, value) in [
            ("url", json!("https://example.invalid/responses")),
            ("headers", json!({"authorization": "secret"})),
            ("unknown", json!(true)),
        ] {
            let mut frame = start_frame();
            frame["data"]["transport"][field] = value;
            let parsed: ClientFrame =
                strict_json::from_slice(&serde_json::to_vec(&frame).unwrap()).unwrap();
            assert_eq!(parsed.into_start().unwrap_err().code, "invalid_turn_start");
        }
    }

    #[test]
    fn unknown_transport_kind_is_rejected() {
        let mut frame = start_frame();
        frame["data"]["transport"]["kind"] = Value::String("https".to_owned());
        let parsed: ClientFrame =
            strict_json::from_slice(&serde_json::to_vec(&frame).unwrap()).unwrap();
        assert_eq!(parsed.into_start().unwrap_err().code, "invalid_turn_start");
    }

    #[test]
    fn mismatched_protocol_fails_closed() {
        let mut frame = start_frame();
        frame["protocol"] = Value::String("other".to_owned());
        let parsed: ClientFrame =
            strict_json::from_slice(&serde_json::to_vec(&frame).unwrap()).unwrap();
        assert_eq!(
            parsed.validate_envelope().unwrap_err().code,
            "protocol_name_mismatch"
        );
    }

    #[test]
    fn correlation_identifiers_use_a_bounded_ascii_alphabet() {
        assert!(valid_id("AZaz09-_.:"));
        assert!(valid_id(&"a".repeat(IDENTIFIER_MAX_BYTES)));
        assert!(!valid_id(""));
        assert!(!valid_id("contains whitespace"));
        assert!(!valid_id("contains/slash"));
        assert!(!valid_id("unicode-☃"));
        assert!(!valid_id(&"a".repeat(IDENTIFIER_MAX_BYTES + 1)));
    }

    #[test]
    fn thinking_levels_include_xhigh_and_max() {
        for (wire, expected) in [
            ("none", ThinkingLevel::None),
            ("low", ThinkingLevel::Low),
            ("medium", ThinkingLevel::Medium),
            ("high", ThinkingLevel::High),
            ("xhigh", ThinkingLevel::Xhigh),
            ("max", ThinkingLevel::Max),
        ] {
            let mut frame = start_frame();
            frame["data"]["options"]["thinking"] = Value::String(wire.to_owned());
            let parsed: ClientFrame =
                strict_json::from_slice(&serde_json::to_vec(&frame).unwrap()).unwrap();
            let start = parsed.into_start().unwrap();
            assert_eq!(
                start.data.options.thinking,
                Some(expected),
                "thinking {wire}"
            );
        }
    }

    #[test]
    fn reasoning_modes_include_standard_and_pro() {
        for (wire, expected) in [
            ("standard", ReasoningMode::Standard),
            ("pro", ReasoningMode::Pro),
        ] {
            let mut frame = start_frame();
            frame["data"]["options"]["reasoningMode"] = Value::String(wire.to_owned());
            let parsed: ClientFrame =
                strict_json::from_slice(&serde_json::to_vec(&frame).unwrap()).unwrap();
            let start = parsed.into_start().unwrap();
            assert_eq!(
                start.data.options.reasoning_mode,
                Some(expected),
                "reasoningMode {wire}"
            );
        }
    }

    #[test]
    fn cancellation_reason_limit_counts_utf8_bytes_and_rejects_controls() {
        for reason in ["é".repeat(64), "a".repeat(CANCEL_REASON_MAX_BYTES)] {
            let mut frame = start_frame();
            frame["type"] = Value::String("turn.cancel".to_owned());
            frame["data"] = json!({"reason": reason});
            let parsed: ClientFrame =
                strict_json::from_slice(&serde_json::to_vec(&frame).unwrap()).unwrap();
            parsed.into_cancel().unwrap();
        }

        for reason in ["é".repeat(65), "line\nbreak".to_owned()] {
            let mut frame = start_frame();
            frame["type"] = Value::String("turn.cancel".to_owned());
            frame["data"] = json!({"reason": reason});
            let parsed: ClientFrame =
                strict_json::from_slice(&serde_json::to_vec(&frame).unwrap()).unwrap();
            assert_eq!(
                parsed.into_cancel().unwrap_err().code,
                "invalid_turn_cancel"
            );
        }
    }
}
