use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ErrorCategory {
    Protocol,
    Config,
    Auth,
    Checkpoint,
    Workspace,
    Provider,
    Tool,
    Cleanup,
    Internal,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RetryDisposition {
    Never,
    Safe,
    After,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct PublicError {
    pub code: String,
    pub category: ErrorCategory,
    pub message: String,
    pub retry: RetryDisposition,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub retry_after_ms: Option<u64>,
}

impl PublicError {
    #[must_use]
    pub fn new(
        code: impl Into<String>,
        category: ErrorCategory,
        message: impl Into<String>,
        retry: RetryDisposition,
    ) -> Self {
        Self {
            code: code.into(),
            category,
            message: bound_message(message.into()),
            retry,
            retry_after_ms: None,
        }
    }

    #[must_use]
    pub fn protocol(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self::new(
            code,
            ErrorCategory::Protocol,
            message,
            RetryDisposition::Never,
        )
    }

    #[must_use]
    pub fn internal(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self::new(
            code,
            ErrorCategory::Internal,
            message,
            RetryDisposition::Safe,
        )
    }
}

fn bound_message(mut message: String) -> String {
    const MAX_CHARS: usize = 512;
    if message.chars().count() <= MAX_CHARS {
        return message;
    }
    let mut end = message.len();
    for (seen, (offset, _)) in message.char_indices().enumerate() {
        if seen == MAX_CHARS {
            end = offset;
            break;
        }
    }
    message.truncate(end);
    message.push('…');
    message
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn public_messages_are_bounded_on_character_boundaries() {
        let error = PublicError::internal("test", "é".repeat(600));
        assert_eq!(error.message.chars().count(), 513);
        assert!(error.message.ends_with('…'));
    }
}
