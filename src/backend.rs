use std::{
    env,
    ffi::OsString,
    fs::OpenOptions,
    io::{ErrorKind, Read, Write},
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use async_trait::async_trait;
use futures_util::StreamExt;
use nanocodex::{
    Nanocodex, NanocodexError, OpenAi, ReasoningMode as NativeReasoningMode,
    Thinking as NativeThinking, Tools, TurnControl, TurnUsage,
    agent::session::SessionSnapshot,
    oai::{
        OpenAiError, ResponseError, ResponseErrorKind,
        auth::{
            ChatGptAuthError, OpenAiAuth, OpenAiAuthError, OpenAiAuthFuture, OpenAiAuthSnapshot,
            OpenAiAuthSource, chatgpt_auth_status, load_chatgpt_auth,
        },
        events::{
            AgentEvent, AgentEventData, AgentEventKind, AssistantEvent, ToolEvent, ToolStatus,
        },
        tower::{ResponsesAttempt, ResponsesServiceFactory, ResponsesServiceResponse},
        transport::{ResponsesError, ResponsesTransport},
    },
};
use serde_json::{Value, json};
use tempfile::{NamedTempFile, TempDir};
use tokio::{
    sync::{Mutex as AsyncMutex, mpsc},
    task::JoinHandle,
};
use tokio_util::sync::CancellationToken;
use tower::Service;

use crate::{
    capabilities::{
        BRIDGE_PROTOCOL_NAME, BRIDGE_PROTOCOL_VERSION, MAX_EVENT_BYTES, MAX_EVENT_TOTAL_BYTES,
        MAX_INPUT_RECORD_BYTES, MAX_MANAGED_AUTH_FILE_BYTES, MAX_PROMPT_BYTES, MAX_SNAPSHOT_BYTES,
        SNAPSHOT_VERSION,
    },
    error::{ErrorCategory, PublicError, RetryDisposition},
    protocol::{
        AuthConfig, Continuation, ModelId, ReasoningMode, ThinkingLevel, TurnOptions, TurnStartData,
    },
};

const EVENT_WIRE_ENVELOPE_RESERVE: usize = 1024;
const EXACT_CANCELLATION_TIMEOUT: Duration = Duration::from_secs(1);
const MAX_MANAGED_AUTH_SYNC_ATTEMPTS: usize = 3;

#[async_trait]
pub trait AcceptedTurnCancellation: Send + Sync + 'static {
    async fn cancel(&self) -> Result<(), PublicError>;
}

#[derive(Clone)]
pub enum BackendNotice {
    Accepted {
        session_id: String,
        cancellation: Arc<dyn AcceptedTurnCancellation>,
    },
    Event {
        event: Value,
    },
    EventTruncated {
        upstream_type: Option<String>,
        upstream_seq: Option<u64>,
        reason: &'static str,
    },
}

struct NanocodexTurnCancellation {
    control: TurnControl,
    issued: Arc<AtomicBool>,
}

#[async_trait]
impl AcceptedTurnCancellation for NanocodexTurnCancellation {
    async fn cancel(&self) -> Result<(), PublicError> {
        if self.issued.swap(true, Ordering::SeqCst) {
            return Ok(());
        }
        self.control.cancel().await.map_err(|error| match error {
            NanocodexError::TurnNotCancellable => PublicError::protocol(
                "turn_not_cancellable",
                "The turn has already entered finalization.",
            ),
            _ => map_nanocodex_error(&error),
        })
    }
}

#[derive(Clone, Debug)]
pub struct CompletedTurn {
    pub session_id: String,
    pub final_message: String,
    pub usage: Value,
    pub snapshot_version: u32,
    pub snapshot: Value,
    pub canonical_workspace: String,
}

#[derive(Clone, Debug)]
pub enum BackendOutcome {
    Completed(CompletedTurn),
    Cancelled {
        session_id: Option<String>,
    },
    Failed {
        session_id: Option<String>,
        error: PublicError,
        completed: Option<CompletedTurn>,
    },
}

#[async_trait]
pub trait AgentBackend: Send + Sync + 'static {
    async fn run(
        &self,
        request: TurnStartData,
        notices: mpsc::Sender<BackendNotice>,
        cancellation: CancellationToken,
    ) -> BackendOutcome;
}

#[derive(Clone, Copy, Debug, Default)]
pub struct NanocodexBackend;

#[async_trait]
impl AgentBackend for NanocodexBackend {
    async fn run(
        &self,
        request: TurnStartData,
        notices: mpsc::Sender<BackendNotice>,
        cancellation: CancellationToken,
    ) -> BackendOutcome {
        run_nanocodex(request, notices, cancellation).await
    }
}

async fn run_nanocodex(
    request: TurnStartData,
    notices: mpsc::Sender<BackendNotice>,
    cancellation: CancellationToken,
) -> BackendOutcome {
    let mut validated = match validate_request(request) {
        Ok(value) => value,
        Err(error) => {
            return BackendOutcome::Failed {
                session_id: None,
                error,
                completed: None,
            };
        }
    };

    let auth = match resolve_auth(&validated.auth) {
        Ok(auth) => auth,
        Err(error) => {
            return BackendOutcome::Failed {
                session_id: None,
                error,
                completed: None,
            };
        }
    };
    validated.tools = match tools_for_auth(&validated.auth) {
        Ok(tools) => tools,
        Err(error) => {
            return BackendOutcome::Failed {
                session_id: None,
                error,
                completed: None,
            };
        }
    };
    let openai = match OpenAi::builder(auth)
        .transport(ResponsesTransport::WebSocket)
        .build()
    {
        Ok(openai) => openai,
        Err(error) => {
            return BackendOutcome::Failed {
                session_id: None,
                error: map_openai_error(&error),
                completed: None,
            };
        }
    };

    run_with_openai(validated, openai, notices, cancellation).await
}

async fn run_with_openai<F>(
    validated: ValidatedRequest,
    openai: OpenAi<F>,
    notices: mpsc::Sender<BackendNotice>,
    cancellation: CancellationToken,
) -> BackendOutcome
where
    F: ResponsesServiceFactory + Send + Sync + 'static,
    F::Service: Service<ResponsesAttempt, Response = ResponsesServiceResponse> + Send + 'static,
    <F::Service as Service<ResponsesAttempt>>::Error: Into<ResponseError> + Send + 'static,
    <F::Service as Service<ResponsesAttempt>>::Future: Send,
{
    let resume_envelope = ResumeEnvelopeContext::from_validated(&validated);
    let mut builder = Nanocodex::builder(openai)
        .workspace(&validated.canonical_workspace)
        .tools(validated.tools);
    if validated.snapshot.is_none() {
        builder = builder.model(native_model(
            validated.options.model.unwrap_or(ModelId::Sol),
        ));
    }
    if let Some(instructions) = validated.options.instructions {
        builder = builder.instructions(instructions);
    }
    if let Some(thinking) = validated.options.thinking {
        builder = builder.thinking(native_thinking(thinking));
    }
    if let Some(reasoning_mode) = validated.options.reasoning_mode {
        builder = builder.reasoning_mode(native_reasoning_mode(reasoning_mode));
    }
    if let Some(fast_mode) = validated.options.fast_mode {
        builder = builder.fast_mode(fast_mode);
    }
    if let Some(snapshot) = validated.snapshot {
        builder = builder.resume(snapshot);
    }

    let (agent, session_events) = match builder.build() {
        Ok(value) => value,
        Err(error) => {
            return BackendOutcome::Failed {
                session_id: None,
                error: map_nanocodex_error(&error),
                completed: None,
            };
        }
    };
    // Protocol v1 consumes the Turn's mirrored event stream. Retaining the
    // independent build-level receiver would buffer the same events without
    // bound because it is never otherwise consumed.
    drop(session_events);
    let session_id = agent.session_id().to_string();

    if cancellation.is_cancelled() {
        let _ = agent.shutdown().await;
        return BackendOutcome::Cancelled {
            session_id: Some(session_id),
        };
    }

    let prompt = agent.prompt(validated.prompt);
    tokio::pin!(prompt);
    let turn = tokio::select! {
        result = &mut prompt => match result {
            Ok(turn) => turn,
            Err(error) => {
                let _ = agent.shutdown().await;
                return BackendOutcome::Failed {
                    session_id: Some(session_id),
                    error: map_nanocodex_error(&error),
                    completed: None,
                };
            }
        },
        () = cancellation.cancelled() => {
            let _ = agent.shutdown().await;
            return BackendOutcome::Cancelled { session_id: Some(session_id) };
        }
    };

    let control = turn.control();
    let exact_cancel_issued = Arc::new(AtomicBool::new(false));
    if notices
        .send(BackendNotice::Accepted {
            session_id: session_id.clone(),
            cancellation: Arc::new(NanocodexTurnCancellation {
                control: control.clone(),
                issued: Arc::clone(&exact_cancel_issued),
            }),
        })
        .await
        .is_err()
    {
        if !exact_cancel_issued.swap(true, Ordering::SeqCst) {
            let _ = tokio::time::timeout(EXACT_CANCELLATION_TIMEOUT, control.cancel()).await;
        }
        let _ = agent.shutdown().await;
        return BackendOutcome::Cancelled {
            session_id: Some(session_id),
        };
    }

    let mut turn = turn;
    let mut forwarder = EventForwarder::new(notices);
    let mut event_error = None;
    let mut cancel_task: Option<JoinHandle<Result<(), NanocodexError>>> = None;
    loop {
        tokio::select! {
            event = turn.next() => {
                let Some(event) = event else {
                    event_error = Some(PublicError::internal(
                        "event_stream_closed",
                        "The agent event stream closed before a terminal event.",
                    ));
                    break;
                };
                let terminal = event.kind.is_terminal();
                if let Err(error) = forwarder.forward(&event)
                    && event_error.is_none()
                {
                    event_error = Some(error);
                }
                if terminal {
                    break;
                }
            }
            () = cancellation.cancelled(), if cancel_task.is_none() => {
                if !exact_cancel_issued.swap(true, Ordering::SeqCst) {
                    let control = control.clone();
                    cancel_task = Some(tokio::spawn(async move { control.cancel().await }));
                }
            }
        }
    }
    if let Err(error) = forwarder.finish().await
        && event_error.is_none()
    {
        event_error = Some(error);
    }
    let cancellation_task_error = if let Some(task) = cancel_task {
        match tokio::time::timeout(EXACT_CANCELLATION_TIMEOUT, task).await {
            Ok(Ok(Ok(()))) | Ok(Ok(Err(NanocodexError::TurnNotCancellable))) => None,
            Ok(Ok(Err(error))) => Some(map_nanocodex_error(&error)),
            Ok(Err(_)) => Some(PublicError::internal(
                "cancellation_task_failed",
                "The turn cancellation task stopped unexpectedly.",
            )),
            Err(_) => None,
        }
    } else {
        None
    };
    let result = turn.await;

    if matches!(result, Err(NanocodexError::TurnCancelled)) {
        // Cancellation is the accepted initiating cause. A later shutdown
        // error is secondary and must not reclassify the public terminal.
        let _ = agent.shutdown().await;
        return BackendOutcome::Cancelled {
            session_id: Some(session_id),
        };
    }

    let result = match result {
        Ok(result) => result,
        Err(error) => {
            let _ = agent.shutdown().await;
            return BackendOutcome::Failed {
                session_id: Some(session_id),
                error: map_nanocodex_error(&error),
                completed: None,
            };
        }
    };

    if let Some(error) = cancellation_task_error.or(event_error) {
        let _ = agent.shutdown().await;
        return BackendOutcome::Failed {
            session_id: Some(session_id),
            error,
            completed: None,
        };
    }

    let snapshot = result.snapshot();
    let snapshot_value = match serde_json::to_value(&snapshot) {
        Ok(value) => value,
        Err(_) => {
            let _ = agent.shutdown().await;
            return BackendOutcome::Failed {
                session_id: Some(session_id),
                error: PublicError::internal(
                    "snapshot_serialization_failed",
                    "The completed session snapshot could not be serialized.",
                ),
                completed: None,
            };
        }
    };
    let snapshot_bytes =
        serde_json::to_vec(&snapshot_value).map_or(usize::MAX, |value| value.len());
    if snapshot_bytes > MAX_SNAPSHOT_BYTES {
        let _ = agent.shutdown().await;
        return BackendOutcome::Failed {
            session_id: Some(session_id),
            error: PublicError::new(
                "snapshot_too_large",
                ErrorCategory::Checkpoint,
                "The completed session snapshot exceeds the bridge limit.",
                RetryDisposition::Never,
            ),
            completed: None,
        };
    }
    if let Err(error) = validate_completed_snapshot_resumability(&snapshot_value, &resume_envelope)
    {
        let _ = agent.shutdown().await;
        return BackendOutcome::Failed {
            session_id: Some(session_id),
            error,
            completed: None,
        };
    }
    let completed = CompletedTurn {
        session_id: session_id.clone(),
        final_message: result.final_message().to_owned(),
        usage: wire_usage(result.usage()),
        snapshot_version: snapshot.version(),
        snapshot: snapshot_value,
        canonical_workspace: snapshot.workspace().to_owned(),
    };

    let shutdown = agent.shutdown().await;
    if let Err(error) = shutdown {
        return BackendOutcome::Failed {
            session_id: Some(session_id),
            error: cleanup_error(&error),
            completed: Some(completed),
        };
    }
    BackendOutcome::Completed(completed)
}

fn validate_completed_snapshot_resumability(
    snapshot: &Value,
    context: &ResumeEnvelopeContext,
) -> Result<(), PublicError> {
    let envelope = context.with_snapshot(snapshot.clone());
    crate::strict_json::validate_value(&envelope).map_err(|_| {
        PublicError::new(
            "snapshot_structure_too_large",
            ErrorCategory::Checkpoint,
            "The completed session snapshot exceeds a structural bridge limit.",
            RetryDisposition::Never,
        )
    })?;
    let encoded = serde_json::to_vec(&envelope).map_err(|_| {
        PublicError::internal(
            "snapshot_resume_validation_failed",
            "The completed session snapshot could not be checked for resume.",
        )
    })?;
    if encoded.len() > MAX_INPUT_RECORD_BYTES {
        return Err(PublicError::new(
            "snapshot_resume_record_too_large",
            ErrorCategory::Checkpoint,
            "The completed session snapshot cannot fit in a resumable command.",
            RetryDisposition::Never,
        ));
    }
    Ok(())
}

struct ResumeEnvelopeContext {
    workspace: String,
    auth: Value,
    options: Value,
}

impl ResumeEnvelopeContext {
    fn from_validated(validated: &ValidatedRequest) -> Self {
        let auth = match &validated.auth {
            AuthConfig::ApiKeyEnv {
                environment_variable,
            } => json!({
                "mode": "api-key-env",
                "environmentVariable": environment_variable,
            }),
            AuthConfig::Chatgpt { auth_file } => json!({
                "mode": "chatgpt",
                "authFile": auth_file
                    .as_ref()
                    .map(|path| path.to_string_lossy().into_owned()),
            }),
        };
        Self {
            workspace: validated.canonical_workspace.to_string_lossy().into_owned(),
            auth,
            options: maximal_resume_options(&validated.options),
        }
    }

    fn with_snapshot(&self, snapshot: Value) -> Value {
        json!({
            "protocol": BRIDGE_PROTOCOL_NAME,
            "version": BRIDGE_PROTOCOL_VERSION,
            "type": "turn.start",
            "commandId": "resume",
            "requestId": "resume",
            "data": {
                "prompt": "r",
                "workspace": self.workspace,
                "auth": self.auth,
                "transport": {"kind": "websocket"},
                "options": self.options,
                "continuation": {"mode": "resume", "snapshot": snapshot},
            },
        })
    }
}

fn maximal_resume_options(options: &TurnOptions) -> Value {
    json!({
        "instructions": options.instructions,
        "thinking": options.thinking.map(|value| match value {
            ThinkingLevel::None => "none",
            ThinkingLevel::Low => "low",
            ThinkingLevel::Medium => "medium",
            ThinkingLevel::High => "high",
            ThinkingLevel::Xhigh => "xhigh",
            ThinkingLevel::Max => "max",
        }),
        "reasoningMode": options.reasoning_mode.map(|value| match value {
            ReasoningMode::Standard => "standard",
            ReasoningMode::Pro => "pro",
        }),
        "fastMode": options.fast_mode,
    })
}

struct ValidatedRequest {
    prompt: String,
    canonical_workspace: PathBuf,
    auth: AuthConfig,
    options: crate::protocol::TurnOptions,
    snapshot: Option<SessionSnapshot>,
    tools: Tools,
}

fn validate_request(request: TurnStartData) -> Result<ValidatedRequest, PublicError> {
    if request.prompt.trim().is_empty() || request.prompt.len() > MAX_PROMPT_BYTES {
        return Err(PublicError::new(
            "invalid_prompt",
            ErrorCategory::Config,
            "The prompt must contain between 1 byte and the advertised prompt limit.",
            RetryDisposition::Never,
        ));
    }
    if !request.workspace.is_absolute() {
        return Err(workspace_error(
            "workspace_not_absolute",
            "The workspace must be absolute.",
        ));
    }
    let canonical_workspace = std::fs::canonicalize(&request.workspace).map_err(|_| {
        workspace_error(
            "workspace_unavailable",
            "The workspace could not be resolved.",
        )
    })?;
    if !canonical_workspace.is_dir() {
        return Err(workspace_error(
            "workspace_not_directory",
            "The workspace is not a directory.",
        ));
    }
    if canonical_workspace.to_str().is_none() {
        return Err(workspace_error(
            "workspace_not_utf8",
            "The canonical workspace is not valid UTF-8.",
        ));
    }
    if request
        .options
        .instructions
        .as_ref()
        .is_some_and(|instructions| {
            instructions.trim().is_empty() || instructions.len() > MAX_PROMPT_BYTES
        })
    {
        return Err(PublicError::new(
            "invalid_instructions",
            ErrorCategory::Config,
            "Explicit instructions must contain between 1 byte and the prompt limit.",
            RetryDisposition::Never,
        ));
    }

    let snapshot = match request.continuation {
        None => None,
        Some(Continuation::Resume { snapshot }) => {
            let encoded = serde_json::to_vec(&snapshot).map_err(|_| {
                PublicError::new(
                    "invalid_snapshot",
                    ErrorCategory::Checkpoint,
                    "The checkpoint snapshot is not valid JSON.",
                    RetryDisposition::Never,
                )
            })?;
            if encoded.len() > MAX_SNAPSHOT_BYTES {
                return Err(PublicError::new(
                    "snapshot_too_large",
                    ErrorCategory::Checkpoint,
                    "The checkpoint snapshot exceeds the bridge limit.",
                    RetryDisposition::Never,
                ));
            }
            let submitted_snapshot = snapshot;
            if !submitted_snapshot
                .get("request_prefix")
                .is_some_and(Value::is_array)
                || submitted_snapshot
                    .get("context_snapshot")
                    .is_none_or(Value::is_null)
            {
                return Err(PublicError::new(
                    "invalid_snapshot",
                    ErrorCategory::Checkpoint,
                    "Only a canonical Nanocodex session snapshot can be resumed.",
                    RetryDisposition::Never,
                ));
            }
            let snapshot: SessionSnapshot = serde_json::from_value(submitted_snapshot.clone())
                .map_err(|_| {
                    PublicError::new(
                        "invalid_snapshot",
                        ErrorCategory::Checkpoint,
                        "The checkpoint snapshot is invalid or unsupported.",
                        RetryDisposition::Never,
                    )
                })?;
            let canonical_snapshot = serde_json::to_value(&snapshot).map_err(|_| {
                PublicError::new(
                    "invalid_snapshot",
                    ErrorCategory::Checkpoint,
                    "The checkpoint snapshot could not be normalized.",
                    RetryDisposition::Never,
                )
            })?;
            if canonical_snapshot != submitted_snapshot {
                return Err(PublicError::new(
                    "invalid_snapshot",
                    ErrorCategory::Checkpoint,
                    "The checkpoint snapshot contains non-canonical or unsupported data.",
                    RetryDisposition::Never,
                ));
            }
            if snapshot.version() != SNAPSHOT_VERSION {
                return Err(PublicError::new(
                    "snapshot_version_unsupported",
                    ErrorCategory::Checkpoint,
                    "The checkpoint snapshot version is not supported.",
                    RetryDisposition::Never,
                ));
            }
            let requested_workspace = canonical_workspace.to_str().ok_or_else(|| {
                workspace_error(
                    "workspace_not_utf8",
                    "The canonical workspace is not valid UTF-8.",
                )
            })?;
            if snapshot.workspace() != requested_workspace {
                return Err(workspace_error(
                    "workspace_changed",
                    "The checkpoint workspace does not exactly match the requested workspace.",
                ));
            }
            let stored_workspace = std::fs::canonicalize(snapshot.workspace()).map_err(|_| {
                workspace_error(
                    "checkpoint_workspace_unavailable",
                    "The checkpoint workspace could not be resolved.",
                )
            })?;
            if stored_workspace != canonical_workspace {
                return Err(workspace_error(
                    "workspace_changed",
                    "The checkpoint workspace does not match the requested workspace.",
                ));
            }
            if let Some(requested) = request.options.model {
                let stored = canonical_snapshot
                    .get("model")
                    .and_then(Value::as_str)
                    .and_then(ModelId::parse);
                if stored != Some(requested) {
                    return Err(PublicError::new(
                        "model_mismatch",
                        ErrorCategory::Checkpoint,
                        "The requested model does not match the resumed snapshot model.",
                        RetryDisposition::Never,
                    ));
                }
            }
            Some(snapshot)
        }
    };

    Ok(ValidatedRequest {
        prompt: request.prompt,
        canonical_workspace,
        auth: request.auth,
        options: request.options,
        snapshot,
        tools: Tools::default(),
    })
}

fn tools_for_auth(config: &AuthConfig) -> Result<Tools, PublicError> {
    let builder = Tools::builder();
    let builder = match config {
        AuthConfig::ApiKeyEnv {
            environment_variable,
        } => builder.process_environment([(environment_variable, "")]),
        AuthConfig::Chatgpt { .. } => builder,
    };
    builder.build().map_err(|_| {
        PublicError::new(
            "invalid_tool_configuration",
            ErrorCategory::Tool,
            "The native tool environment could not be configured.",
            RetryDisposition::Never,
        )
    })
}

fn resolve_auth(config: &AuthConfig) -> Result<OpenAiAuth, PublicError> {
    match config {
        AuthConfig::ApiKeyEnv {
            environment_variable,
        } => {
            if !valid_environment_name(environment_variable) {
                return Err(PublicError::new(
                    "invalid_auth_environment",
                    ErrorCategory::Auth,
                    "The API-key environment variable name is invalid.",
                    RetryDisposition::Never,
                ));
            }
            let key = env::var(environment_variable).map_err(|_| {
                PublicError::new(
                    "auth_unavailable",
                    ErrorCategory::Auth,
                    "The configured API-key environment variable is unavailable.",
                    RetryDisposition::Never,
                )
            })?;
            if key.trim().is_empty() {
                return Err(PublicError::new(
                    "auth_unavailable",
                    ErrorCategory::Auth,
                    "The configured API-key environment variable is empty.",
                    RetryDisposition::Never,
                ));
            }
            Ok(OpenAiAuth::api_key(key))
        }
        AuthConfig::Chatgpt { auth_file } => {
            let auth_file = auth_file
                .clone()
                .or_else(default_chatgpt_auth_file)
                .ok_or_else(|| {
                    PublicError::new(
                        "auth_file_unavailable",
                        ErrorCategory::Auth,
                        "A managed ChatGPT authentication file could not be located.",
                        RetryDisposition::Never,
                    )
                })?;
            load_bounded_chatgpt_auth(&auth_file)
        }
    }
}

#[derive(Debug, thiserror::Error)]
enum ManagedAuthFileError {
    #[error("managed authentication I/O failed: {0}")]
    Io(#[from] std::io::Error),
    #[error("managed authentication target is not a regular file")]
    InvalidType,
    #[error("managed authentication file exceeds its byte limit")]
    TooLarge,
    #[error("managed authentication path has no parent directory")]
    MissingParent,
    #[error("managed authentication staging file could not be persisted: {0}")]
    Persist(#[from] tempfile::PersistError),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum AuthSyncDisposition {
    Unchanged,
    UpdatedOriginal,
    AdoptedExternal,
}

struct BoundedManagedAuthStore {
    original: PathBuf,
    staged: PathBuf,
    baseline: Vec<u8>,
}

impl BoundedManagedAuthStore {
    fn sync_from_original(&mut self) -> Result<AuthSyncDisposition, ManagedAuthFileError> {
        let current = read_bounded_regular_file(&self.original)?;
        if current == self.baseline {
            return Ok(AuthSyncDisposition::Unchanged);
        }
        atomic_write_managed_auth(&self.staged, &current)?;
        self.baseline = current;
        Ok(AuthSyncDisposition::AdoptedExternal)
    }

    fn sync_after_inner(&mut self) -> Result<AuthSyncDisposition, ManagedAuthFileError> {
        let original = read_bounded_regular_file(&self.original)?;
        let staged = read_bounded_regular_file(&self.staged)?;

        if original != self.baseline {
            if staged != original {
                atomic_write_managed_auth(&self.staged, &original)?;
            }
            self.baseline = original;
            return Ok(AuthSyncDisposition::AdoptedExternal);
        }
        if staged != self.baseline {
            if atomic_write_managed_auth_if_unchanged(&self.original, &self.baseline, &staged)? {
                self.baseline = staged;
                return Ok(AuthSyncDisposition::UpdatedOriginal);
            }
            let external = read_bounded_regular_file(&self.original)?;
            atomic_write_managed_auth(&self.staged, &external)?;
            self.baseline = external;
            return Ok(AuthSyncDisposition::AdoptedExternal);
        }
        Ok(AuthSyncDisposition::Unchanged)
    }
}

struct BoundedManagedAuthSource {
    inner: AsyncMutex<OpenAiAuth>,
    store: Mutex<BoundedManagedAuthStore>,
    reload_required: Mutex<bool>,
    account_id: String,
    _staging_directory: TempDir,
}

impl BoundedManagedAuthSource {
    fn sync_from_original(&self) -> Result<AuthSyncDisposition, OpenAiAuthError> {
        let disposition = self
            .store
            .lock()
            .map_err(|_| managed_auth_store_unavailable())?
            .sync_from_original()
            .map_err(|_| managed_auth_store_unavailable())?;
        self.record_reload_requirement(disposition)?;
        Ok(disposition)
    }

    fn sync_after_inner(&self) -> Result<AuthSyncDisposition, OpenAiAuthError> {
        let disposition = self
            .store
            .lock()
            .map_err(|_| managed_auth_store_unavailable())?
            .sync_after_inner()
            .map_err(|_| managed_auth_store_unavailable())?;
        self.record_reload_requirement(disposition)?;
        Ok(disposition)
    }

    fn record_reload_requirement(
        &self,
        disposition: AuthSyncDisposition,
    ) -> Result<(), OpenAiAuthError> {
        if disposition == AuthSyncDisposition::AdoptedExternal {
            *self
                .reload_required
                .lock()
                .map_err(|_| managed_auth_store_unavailable())? = true;
        }
        Ok(())
    }

    fn load_staged_inner(&self) -> Result<OpenAiAuth, OpenAiAuthError> {
        let staged = self
            .store
            .lock()
            .map_err(|_| managed_auth_store_unavailable())?
            .staged
            .clone();
        let status = chatgpt_auth_status(&staged).map_err(|_| managed_auth_store_unavailable())?;
        if status.account_id != self.account_id {
            return Err(OpenAiAuthError::AccountChanged);
        }
        load_chatgpt_auth(staged).map_err(|_| managed_auth_store_unavailable())
    }

    fn reload_inner_if_required(&self, inner: &mut OpenAiAuth) -> Result<(), OpenAiAuthError> {
        let required = *self
            .reload_required
            .lock()
            .map_err(|_| managed_auth_store_unavailable())?;
        if !required {
            return Ok(());
        }
        let replacement = self.load_staged_inner()?;
        *inner = replacement;
        *self
            .reload_required
            .lock()
            .map_err(|_| managed_auth_store_unavailable())? = false;
        Ok(())
    }
}

impl OpenAiAuthSource for BoundedManagedAuthSource {
    fn validate(&self) -> Result<(), OpenAiAuthError> {
        self.inner
            .try_lock()
            .map_err(|_| managed_auth_store_unavailable())?
            .validate()
    }

    fn snapshot(&self) -> OpenAiAuthFuture<'_, Result<OpenAiAuthSnapshot, OpenAiAuthError>> {
        Box::pin(async move {
            let mut inner = self.inner.lock().await;
            for _ in 0..MAX_MANAGED_AUTH_SYNC_ATTEMPTS {
                self.sync_from_original()?;
                self.reload_inner_if_required(&mut inner)?;
                let result = inner.snapshot().await;
                if self.sync_after_inner()? == AuthSyncDisposition::AdoptedExternal {
                    self.reload_inner_if_required(&mut inner)?;
                    continue;
                }
                return result;
            }
            Err(managed_auth_store_unavailable())
        })
    }

    fn recover_unauthorized(
        &self,
        rejected: &OpenAiAuthSnapshot,
    ) -> OpenAiAuthFuture<'_, Result<(), OpenAiAuthError>> {
        let rejected = rejected.clone();
        Box::pin(async move {
            let mut inner = self.inner.lock().await;
            let adopted_before = self.sync_from_original()? == AuthSyncDisposition::AdoptedExternal;
            self.reload_inner_if_required(&mut inner)?;
            let result = if adopted_before {
                Ok(())
            } else {
                inner.recover_unauthorized(&rejected).await
            };
            if self.sync_after_inner()? == AuthSyncDisposition::AdoptedExternal {
                self.reload_inner_if_required(&mut inner)?;
                return Ok(());
            }
            result
        })
    }
}

fn load_bounded_chatgpt_auth(path: &Path) -> Result<OpenAiAuth, PublicError> {
    let canonical = std::fs::canonicalize(path)
        .map_err(ManagedAuthFileError::Io)
        .map_err(map_managed_auth_file_error)?;
    let initial = read_bounded_regular_file(&canonical).map_err(map_managed_auth_file_error)?;
    let staging_directory = tempfile::Builder::new()
        .prefix("smithers-nanocodex-auth-")
        .tempdir()
        .map_err(ManagedAuthFileError::Io)
        .map_err(map_managed_auth_file_error)?;
    let staged = staging_directory.path().join("auth.json");
    atomic_write_managed_auth(&staged, &initial).map_err(map_managed_auth_file_error)?;
    let status = chatgpt_auth_status(&staged).map_err(|error| map_chatgpt_auth_error(&error))?;
    let inner = load_chatgpt_auth(&staged).map_err(|error| map_chatgpt_auth_error(&error))?;
    let source = BoundedManagedAuthSource {
        inner: AsyncMutex::new(inner),
        store: Mutex::new(BoundedManagedAuthStore {
            original: canonical,
            staged,
            baseline: initial,
        }),
        reload_required: Mutex::new(false),
        account_id: status.account_id,
        _staging_directory: staging_directory,
    };
    Ok(OpenAiAuth::managed_chatgpt(Arc::new(source)))
}

#[cfg(test)]
fn validate_managed_auth_file(path: &Path) -> Result<(), PublicError> {
    let canonical = std::fs::canonicalize(path)
        .map_err(ManagedAuthFileError::Io)
        .map_err(map_managed_auth_file_error)?;
    read_bounded_regular_file(&canonical)
        .map(|_| ())
        .map_err(map_managed_auth_file_error)
}

fn read_bounded_regular_file(path: &Path) -> Result<Vec<u8>, ManagedAuthFileError> {
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_NONBLOCK);
    }
    let file = options.open(path)?;
    let metadata = file.metadata()?;
    if !metadata.file_type().is_file() {
        return Err(ManagedAuthFileError::InvalidType);
    }
    read_bounded_managed_auth(file, metadata.len())
}

fn read_bounded_managed_auth(
    reader: impl Read,
    advertised_len: u64,
) -> Result<Vec<u8>, ManagedAuthFileError> {
    if advertised_len > MAX_MANAGED_AUTH_FILE_BYTES as u64 {
        return Err(ManagedAuthFileError::TooLarge);
    }

    let mut bytes = Vec::with_capacity(
        usize::try_from(advertised_len)
            .unwrap_or(MAX_MANAGED_AUTH_FILE_BYTES)
            .min(MAX_MANAGED_AUTH_FILE_BYTES),
    );
    reader
        .take(MAX_MANAGED_AUTH_FILE_BYTES as u64 + 1)
        .read_to_end(&mut bytes)?;
    if bytes.len() > MAX_MANAGED_AUTH_FILE_BYTES {
        return Err(ManagedAuthFileError::TooLarge);
    }
    Ok(bytes)
}

fn atomic_write_managed_auth(path: &Path, bytes: &[u8]) -> Result<(), ManagedAuthFileError> {
    prepare_managed_auth_replacement(path, bytes)?.persist(path)?;
    Ok(())
}

fn atomic_write_managed_auth_if_unchanged(
    path: &Path,
    expected: &[u8],
    bytes: &[u8],
) -> Result<bool, ManagedAuthFileError> {
    let temporary = prepare_managed_auth_replacement(path, bytes)?;
    if read_bounded_regular_file(path)? != expected {
        return Ok(false);
    }
    temporary.persist(path)?;
    Ok(true)
}

fn prepare_managed_auth_replacement(
    path: &Path,
    bytes: &[u8],
) -> Result<NamedTempFile, ManagedAuthFileError> {
    if bytes.len() > MAX_MANAGED_AUTH_FILE_BYTES {
        return Err(ManagedAuthFileError::TooLarge);
    }
    let parent = path.parent().ok_or(ManagedAuthFileError::MissingParent)?;
    let mut temporary = NamedTempFile::new_in(parent)?;
    temporary.as_file_mut().write_all(bytes)?;
    temporary.as_file_mut().sync_all()?;
    Ok(temporary)
}

fn map_managed_auth_file_error(error: ManagedAuthFileError) -> PublicError {
    let (code, message) = match error {
        ManagedAuthFileError::InvalidType => (
            "invalid_auth_file_type",
            "The managed authentication path must resolve to a regular file.",
        ),
        ManagedAuthFileError::TooLarge => (
            "auth_file_too_large",
            "The managed authentication file exceeds the bridge limit.",
        ),
        ManagedAuthFileError::Io(ref error) if error.kind() == ErrorKind::PermissionDenied => (
            "auth_file_unreadable",
            "The managed authentication file is unreadable.",
        ),
        ManagedAuthFileError::Io(ref error) if error.kind() == ErrorKind::NotFound => (
            "auth_file_unavailable",
            "The managed authentication file is unavailable.",
        ),
        _ => (
            "auth_file_unreadable",
            "The managed authentication file could not be staged safely.",
        ),
    };
    PublicError::new(code, ErrorCategory::Auth, message, RetryDisposition::Never)
}

fn managed_auth_store_unavailable() -> OpenAiAuthError {
    OpenAiAuthError::Unavailable(Arc::from(
        "the bounded managed authentication store is unavailable",
    ))
}

fn default_chatgpt_auth_file() -> Option<PathBuf> {
    default_chatgpt_auth_file_from(
        env::var_os("NANOCODEX_AUTH_FILE"),
        env::var_os("CODEX_HOME"),
        env::var_os("HOME"),
        env::var_os("USERPROFILE"),
    )
}

fn default_chatgpt_auth_file_from(
    nanocodex_auth_file: Option<OsString>,
    codex_home: Option<OsString>,
    home: Option<OsString>,
    user_profile: Option<OsString>,
) -> Option<PathBuf> {
    if let Some(path) = nanocodex_auth_file {
        return Some(PathBuf::from(path));
    }
    if let Some(path) = codex_home.filter(|path| !path.is_empty()) {
        return Some(PathBuf::from(path).join("auth.json"));
    }
    home.or(user_profile)
        .map(|home| PathBuf::from(home).join(".codex/auth.json"))
}

fn wire_usage(usage: &TurnUsage) -> Value {
    let estimated_cost = usage.estimated_cost();
    serde_json::json!({
        "inputTokens": usage.input_tokens(),
        "cachedInputTokens": usage.cached_input_tokens(),
        "cacheWriteInputTokens": usage.cache_write_input_tokens(),
        "outputTokens": usage.output_tokens(),
        "reasoningOutputTokens": usage.reasoning_output_tokens(),
        "totalTokens": usage.total_tokens(),
        "estimatedUsd": estimated_cost.map(|cost| cost.amount().decimal()),
        "costStatus": usage.cost_status().as_str(),
        "serviceTier": estimated_cost.map(|cost| cost.service_tier().as_str()),
    })
}

fn valid_environment_name(value: &str) -> bool {
    let mut bytes = value.bytes();
    let Some(first) = bytes.next() else {
        return false;
    };
    value.len() <= 128
        && (first == b'_' || first.is_ascii_alphabetic())
        && bytes.all(|byte| byte == b'_' || byte.is_ascii_alphanumeric())
}

fn native_model(value: ModelId) -> nanocodex::Model {
    match value {
        ModelId::Sol => nanocodex::Model::Sol,
        ModelId::Terra => nanocodex::Model::Terra,
        ModelId::Luna => nanocodex::Model::Luna,
    }
}

fn native_thinking(value: ThinkingLevel) -> NativeThinking {
    match value {
        ThinkingLevel::None => NativeThinking::None,
        ThinkingLevel::Low => NativeThinking::Low,
        ThinkingLevel::Medium => NativeThinking::Medium,
        ThinkingLevel::High => NativeThinking::High,
        ThinkingLevel::Xhigh => NativeThinking::Xhigh,
        ThinkingLevel::Max => NativeThinking::Max,
    }
}

fn native_reasoning_mode(value: ReasoningMode) -> NativeReasoningMode {
    match value {
        ReasoningMode::Standard => NativeReasoningMode::Standard,
        ReasoningMode::Pro => NativeReasoningMode::Pro,
    }
}

struct EventForwarder {
    notices: mpsc::Sender<BackendNotice>,
    forwarded_bytes: usize,
    total_truncation_reported: bool,
    backpressure_truncation_pending: bool,
}

impl EventForwarder {
    fn new(notices: mpsc::Sender<BackendNotice>) -> Self {
        Self {
            notices,
            forwarded_bytes: 0,
            total_truncation_reported: false,
            backpressure_truncation_pending: false,
        }
    }

    fn forward(&mut self, event: &nanocodex::oai::events::AgentEvent) -> Result<(), PublicError> {
        if self.backpressure_truncation_pending {
            match self.notices.try_send(backpressure_truncation_notice()) {
                Ok(()) => self.backpressure_truncation_pending = false,
                Err(mpsc::error::TrySendError::Full(_)) => return Ok(()),
                Err(mpsc::error::TrySendError::Closed(_)) => {
                    return Err(notice_channel_closed());
                }
            }
        }
        let upstream_type = Some(event_kind_name(event.kind)?);
        let upstream_seq = Some(event.seq);
        let (notice, charge) = if event
            .payload
            .get()
            .len()
            .saturating_add(EVENT_WIRE_ENVELOPE_RESERVE)
            > MAX_EVENT_BYTES
        {
            (
                BackendNotice::EventTruncated {
                    upstream_type: upstream_type.clone(),
                    upstream_seq,
                    reason: "event_limit",
                },
                EVENT_WIRE_ENVELOPE_RESERVE,
            )
        } else if matches!(
            event.kind,
            AgentEventKind::ApiEvent | AgentEventKind::ReasoningSummaryDelta
        ) {
            (
                BackendNotice::EventTruncated {
                    upstream_type: upstream_type.clone(),
                    upstream_seq,
                    reason: "event_policy",
                },
                EVENT_WIRE_ENVELOPE_RESERVE,
            )
        } else {
            match safe_event_projection(event) {
                Ok(value) => {
                    let size = serde_json::to_vec(&value)
                        .map_err(|_| event_serialization_error())?
                        .len()
                        .saturating_add(EVENT_WIRE_ENVELOPE_RESERVE);
                    if size > MAX_EVENT_BYTES {
                        (
                            BackendNotice::EventTruncated {
                                upstream_type: upstream_type.clone(),
                                upstream_seq,
                                reason: "event_limit",
                            },
                            EVENT_WIRE_ENVELOPE_RESERVE,
                        )
                    } else {
                        (BackendNotice::Event { event: value }, size)
                    }
                }
                Err(_) => (
                    BackendNotice::EventTruncated {
                        upstream_type: upstream_type.clone(),
                        upstream_seq,
                        reason: "event_policy",
                    },
                    EVENT_WIRE_ENVELOPE_RESERVE,
                ),
            }
        };
        let notice = if self
            .forwarded_bytes
            .saturating_add(charge)
            .saturating_add(EVENT_WIRE_ENVELOPE_RESERVE)
            > MAX_EVENT_TOTAL_BYTES
        {
            if !self.total_truncation_reported {
                self.total_truncation_reported = true;
                BackendNotice::EventTruncated {
                    upstream_type,
                    upstream_seq,
                    reason: "aggregate_event_limit",
                }
            } else {
                return Ok(());
            }
        } else {
            self.forwarded_bytes += charge;
            notice
        };
        match self.notices.try_send(notice) {
            Ok(()) => {}
            Err(mpsc::error::TrySendError::Full(_)) => {
                self.backpressure_truncation_pending = true;
            }
            Err(mpsc::error::TrySendError::Closed(_)) => return Err(notice_channel_closed()),
        }
        Ok(())
    }

    async fn finish(&mut self) -> Result<(), PublicError> {
        if self.backpressure_truncation_pending {
            self.notices
                .send(backpressure_truncation_notice())
                .await
                .map_err(|_| notice_channel_closed())?;
            self.backpressure_truncation_pending = false;
        }
        Ok(())
    }
}

fn projects_typed_payload(kind: AgentEventKind) -> bool {
    matches!(
        kind,
        AgentEventKind::AssistantDelta
            | AgentEventKind::AssistantMessage
            | AgentEventKind::ToolCall
            | AgentEventKind::ToolResult
    )
}

fn safe_event_projection(event: &AgentEvent) -> Result<Value, PublicError> {
    let payload = if projects_typed_payload(event.kind) {
        match event.data().map_err(|_| event_serialization_error())? {
            AgentEventData::Assistant(AssistantEvent::Delta(delta)) => json!({
                "modelCallIndex": delta.model_call_index,
                "itemId": delta.item_id,
                "phase": delta.phase,
                "text": delta.text,
            }),
            AgentEventData::Assistant(AssistantEvent::Message(message)) => json!({
                "modelCallIndex": message.model_call_index,
                "itemId": message.item_id,
                "phase": message.phase,
                "text": message.text,
            }),
            AgentEventData::Tool(ToolEvent::Call(call)) => json!({
                "callId": call.call_id,
                "tool": call.tool,
                "modelCallIndex": call.model_call_index,
            }),
            AgentEventData::Tool(ToolEvent::Result(result)) => json!({
                "callId": result.call_id,
                "tool": result.tool,
                "status": tool_status_name(result.status),
                "durationNs": result.duration_ns,
                "startedAfterNs": result.started_after_ns,
            }),
            _ => json!({}),
        }
    } else {
        json!({})
    };
    Ok(json!({
        "type": event_kind_name(event.kind)?,
        "upstreamSeq": event.seq,
        "payload": payload,
    }))
}

fn event_kind_name(kind: AgentEventKind) -> Result<String, PublicError> {
    serde_json::to_value(kind)
        .ok()
        .and_then(|value| value.as_str().map(str::to_owned))
        .ok_or_else(event_serialization_error)
}

fn tool_status_name(status: ToolStatus) -> &'static str {
    match status {
        ToolStatus::Completed => "completed",
        ToolStatus::Failed => "failed",
        ToolStatus::Cancelled => "cancelled",
        _ => "unknown",
    }
}

fn event_serialization_error() -> PublicError {
    PublicError::internal(
        "event_serialization_failed",
        "An agent event could not be serialized.",
    )
}

fn backpressure_truncation_notice() -> BackendNotice {
    BackendNotice::EventTruncated {
        upstream_type: None,
        upstream_seq: None,
        reason: "bridge_event_backpressure",
    }
}

fn notice_channel_closed() -> PublicError {
    PublicError::internal(
        "event_output_closed",
        "The bridge event output channel closed unexpectedly.",
    )
}

fn map_chatgpt_auth_error(error: &ChatGptAuthError) -> PublicError {
    match error {
        ChatGptAuthError::Storage { source, .. } => {
            let (code, message) = if source.kind() == ErrorKind::PermissionDenied {
                (
                    "auth_file_unreadable",
                    "The managed authentication file is not readable.",
                )
            } else {
                (
                    "auth_file_unavailable",
                    "The managed authentication file is unavailable.",
                )
            };
            PublicError::new(code, ErrorCategory::Auth, message, RetryDisposition::Never)
        }
        ChatGptAuthError::InvalidStore { .. } | ChatGptAuthError::InvalidToken(_) => {
            PublicError::new(
                "invalid_auth_file",
                ErrorCategory::Auth,
                "The managed authentication file does not contain a valid ChatGPT session.",
                RetryDisposition::Never,
            )
        }
        ChatGptAuthError::CallbackUnavailable
        | ChatGptAuthError::CallbackTimeout
        | ChatGptAuthError::StateMismatch
        | ChatGptAuthError::LoginRejected(_)
        | ChatGptAuthError::TokenExchange(_) => PublicError::new(
            "auth_login_failed",
            ErrorCategory::Auth,
            "Managed ChatGPT login did not complete successfully.",
            RetryDisposition::Never,
        ),
    }
}

fn map_openai_auth_error(error: &OpenAiAuthError) -> PublicError {
    match error {
        OpenAiAuthError::Empty => PublicError::new(
            "auth_unavailable",
            ErrorCategory::Auth,
            "The configured provider credential is empty.",
            RetryDisposition::Never,
        ),
        OpenAiAuthError::Unavailable(_) => PublicError::new(
            "auth_temporarily_unavailable",
            ErrorCategory::Auth,
            "Managed provider authentication is temporarily unavailable.",
            RetryDisposition::Safe,
        ),
        OpenAiAuthError::AccountChanged => PublicError::new(
            "auth_account_changed",
            ErrorCategory::Auth,
            "The managed ChatGPT account changed while the turn was active.",
            RetryDisposition::Never,
        ),
        OpenAiAuthError::LoginRequired(_) => PublicError::new(
            "auth_login_required",
            ErrorCategory::Auth,
            "Managed ChatGPT authentication requires a new interactive login.",
            RetryDisposition::Never,
        ),
        OpenAiAuthError::Refresh(_) => PublicError::new(
            "auth_refresh_failed",
            ErrorCategory::Auth,
            "Managed ChatGPT authentication could not be refreshed.",
            RetryDisposition::Safe,
        ),
    }
}

fn map_openai_error(error: &OpenAiError) -> PublicError {
    match error {
        OpenAiError::Authorization(error) => map_openai_auth_error(error),
        OpenAiError::InvalidConfiguration { .. } => PublicError::new(
            "invalid_provider_configuration",
            ErrorCategory::Config,
            "The provider transport configuration is invalid.",
            RetryDisposition::Never,
        ),
    }
}

fn map_nanocodex_error(error: &NanocodexError) -> PublicError {
    if let Some(provider) = error.responses_error() {
        if provider.is_checkpoint_missing() {
            return PublicError::new(
                "checkpoint_missing",
                ErrorCategory::Checkpoint,
                "The provider no longer recognizes the checkpoint required by this turn.",
                RetryDisposition::Never,
            );
        }
        let class = provider.class();
        let category = if matches!(
            provider,
            ResponsesError::Authorization { .. }
                | ResponsesError::InvalidAuthorization { .. }
                | ResponsesError::HandshakeRejected { status: 401, .. }
                | ResponsesError::HttpRejected {
                    status: 401 | 403,
                    ..
                }
        ) {
            ErrorCategory::Auth
        } else {
            ErrorCategory::Provider
        };
        let mut public = PublicError::new(
            format!("provider_{class}"),
            category,
            "The model provider request failed.",
            if provider.retry_advice().is_some() {
                RetryDisposition::Safe
            } else {
                RetryDisposition::Never
            },
        );
        if let Some(advice) = provider.retry_advice()
            && let Some(delay) = advice.server_delay
        {
            match u64::try_from(delay.as_millis()) {
                Ok(milliseconds) => {
                    public.retry = RetryDisposition::After;
                    public.retry_after_ms = Some(milliseconds);
                }
                Err(_) => public.retry = RetryDisposition::Safe,
            }
        }
        return public;
    }
    match error {
        NanocodexError::Response(response) => match response.kind() {
            ResponseErrorKind::ContextWindowExceeded => PublicError::new(
                "provider_context_window_exceeded",
                ErrorCategory::Provider,
                "The model provider rejected the request because its context window was exceeded.",
                RetryDisposition::Never,
            ),
            ResponseErrorKind::Service => PublicError::new(
                "provider_service",
                ErrorCategory::Provider,
                "The model provider service failed without typed retry guidance.",
                RetryDisposition::Never,
            ),
            ResponseErrorKind::Protocol => PublicError::internal(
                "provider_protocol_failed",
                "The model provider response violated the upstream response protocol.",
            ),
            _ => PublicError::internal(
                "provider_response_failed",
                "The model provider response failed with an unknown classification.",
            ),
        },
        NanocodexError::InvalidRequest(_) => PublicError::new(
            "invalid_agent_configuration",
            ErrorCategory::Config,
            "The requested agent policy is invalid or incompatible.",
            RetryDisposition::Never,
        ),
        NanocodexError::WorkspaceChanged { .. } => workspace_error(
            "workspace_changed",
            "The checkpoint workspace does not match the requested workspace.",
        ),
        NanocodexError::ResolveWorkspace { .. }
        | NanocodexError::WorkspaceNotDirectory { .. }
        | NanocodexError::WorkspaceNotUtf8 { .. } => workspace_error(
            "workspace_invalid",
            "The requested workspace is invalid or unavailable.",
        ),
        NanocodexError::InvalidSessionSnapshot(_) => PublicError::new(
            "invalid_snapshot",
            ErrorCategory::Checkpoint,
            "The checkpoint snapshot is invalid or incompatible with the current agent policy.",
            RetryDisposition::Never,
        ),
        NanocodexError::CheckpointLineageMismatch => PublicError::new(
            "checkpoint_lineage_mismatch",
            ErrorCategory::Checkpoint,
            "The completed checkpoint belongs to a different conversation lineage.",
            RetryDisposition::Never,
        ),
        NanocodexError::ForkBeforeCompletedTurn => PublicError::new(
            "checkpoint_unavailable",
            ErrorCategory::Checkpoint,
            "No completed checkpoint is available for this operation.",
            RetryDisposition::Never,
        ),
        NanocodexError::Tools(_) => PublicError::new(
            "invalid_tool_configuration",
            ErrorCategory::Tool,
            "The configured native tool runtime could not be built.",
            RetryDisposition::Never,
        ),
        NanocodexError::TurnCancelled => PublicError::new(
            "turn_cancelled",
            ErrorCategory::Internal,
            "The turn was cancelled.",
            RetryDisposition::Safe,
        ),
        NanocodexError::AgentStopped => PublicError::internal(
            "agent_stopped",
            "The Nanocodex agent stopped before accepting the turn.",
        ),
        NanocodexError::TurnStopped => PublicError::internal(
            "turn_stopped",
            "The Nanocodex agent stopped before the turn completed.",
        ),
        NanocodexError::Event(_) => PublicError::internal(
            "event_protocol_failed",
            "The Nanocodex event stream violated its contract.",
        ),
        NanocodexError::MalformedResponse { .. } | NanocodexError::InvalidAttemptState { .. } => {
            PublicError::new(
                "provider_protocol_failed",
                ErrorCategory::Provider,
                "The model provider response violated the upstream response protocol.",
                RetryDisposition::Never,
            )
        }
        NanocodexError::SerializePromptPrefix(_) => PublicError::new(
            "invalid_agent_configuration",
            ErrorCategory::Config,
            "The requested agent policy is invalid or incompatible.",
            RetryDisposition::Never,
        ),
        NanocodexError::TurnNotCancellable => PublicError::protocol(
            "turn_not_cancellable",
            "The turn has already entered finalization.",
        ),
        NanocodexError::TurnNotSteerable | NanocodexError::SteerQueueFull => PublicError::new(
            "steering_unsupported",
            ErrorCategory::Config,
            "The requested agent policy is invalid or incompatible.",
            RetryDisposition::Never,
        ),
        NanocodexError::TokioRuntimeUnavailable => PublicError::new(
            "invalid_agent_configuration",
            ErrorCategory::Config,
            "The requested agent policy is invalid or incompatible.",
            RetryDisposition::Never,
        ),
        NanocodexError::InitializeRollout { .. } | NanocodexError::PersistRollout { .. } => {
            PublicError::new(
                "nanocodex_failed",
                ErrorCategory::Internal,
                "The Nanocodex agent failed.",
                RetryDisposition::Never,
            )
        }
        NanocodexError::Shutdown(_) => cleanup_error(error),
    }
}

fn cleanup_error(_error: &NanocodexError) -> PublicError {
    PublicError::new(
        "cleanup_failed",
        ErrorCategory::Cleanup,
        "The Nanocodex agent did not shut down cleanly.",
        RetryDisposition::Safe,
    )
}

fn workspace_error(code: &'static str, message: &'static str) -> PublicError {
    PublicError::new(
        code,
        ErrorCategory::Workspace,
        message,
        RetryDisposition::Never,
    )
}

#[cfg(test)]
mod tests {
    use std::{
        io::ErrorKind,
        path::{Path, PathBuf},
        process::{Command, Stdio},
        sync::{
            Arc, Mutex,
            atomic::{AtomicUsize, Ordering},
        },
        time::Duration,
    };

    use nanocodex::oai::{
        ResponseError,
        events::EventError,
        responses::{ContentItem, MessageRole, ResponseItem, WarmupResponse},
        tower::{
            CodeCall, CodeCallKind, GenerationOutput, ResponsePipelineStats, ResponsesAttemptKind,
            ResponsesOutput, ResponsesServiceResponse,
        },
        transport::ResponsesError,
    };
    use serde_json::json;
    use tower::service_fn;

    use super::*;

    const FRESH_PROCESS_GUARD_ENV: &str = "SMITHERS_NANOCODEX_TEST_FRESH_PROCESS_GUARD";
    const FRESH_PROCESS_PHASE_ENV: &str = "SMITHERS_NANOCODEX_TEST_FRESH_PROCESS_PHASE";
    const FRESH_PROCESS_WORKSPACE_ENV: &str = "SMITHERS_NANOCODEX_TEST_FRESH_PROCESS_WORKSPACE";
    const FRESH_PROCESS_SNAPSHOT_ENV: &str = "SMITHERS_NANOCODEX_TEST_FRESH_PROCESS_SNAPSHOT";
    const FRESH_PROCESS_OBSERVED_ENV: &str = "SMITHERS_NANOCODEX_TEST_FRESH_PROCESS_OBSERVED";
    const FRESH_PROCESS_GUARD: &str = "smithers-nanocodex-session-snapshot-child-v1";
    const FRESH_PROCESS_TEST: &str =
        "backend::tests::session_snapshot_resumes_across_fresh_os_processes";
    const TOOL_ENV_GUARD_ENV: &str = "SMITHERS_NANOCODEX_TEST_TOOL_ENV_GUARD";
    const TOOL_ENV_WORKSPACE_ENV: &str = "SMITHERS_NANOCODEX_TEST_TOOL_ENV_WORKSPACE";
    const TOOL_ENV_GUARD: &str = "smithers-nanocodex-tool-environment-child-v1";
    const TOOL_ENV_TEST: &str =
        "backend::tests::configured_api_key_env_is_empty_in_native_tool_process";
    const CUSTOM_API_KEY_ENV: &str = "SMITHERS_PROVIDER_VALUE";
    const UNRELATED_TOOL_ENV: &str = "SMITHERS_UNRELATED_VALUE";

    struct NoopCancellation;

    #[async_trait]
    impl AcceptedTurnCancellation for NoopCancellation {
        async fn cancel(&self) -> Result<(), PublicError> {
            Ok(())
        }
    }

    fn validated_request(
        workspace: &std::path::Path,
        prompt: &str,
        snapshot: Option<SessionSnapshot>,
    ) -> ValidatedRequest {
        ValidatedRequest {
            prompt: prompt.to_owned(),
            canonical_workspace: workspace.canonicalize().unwrap(),
            auth: AuthConfig::ApiKeyEnv {
                environment_variable: "UNUSED_TEST_KEY".to_owned(),
            },
            options: crate::protocol::TurnOptions {
                instructions: Some("stable deterministic instructions".to_owned()),
                ..Default::default()
            },
            snapshot,
            tools: Tools::default(),
        }
    }

    fn deterministic_generation(response_id: &str, text: &str) -> ResponsesOutput {
        ResponsesOutput::Generation(GenerationOutput {
            id: response_id.to_owned(),
            status: "completed".to_owned(),
            end_turn: Some(true),
            final_message: Some(text.to_owned()),
            output_items: vec![ResponseItem::message(
                MessageRole::Assistant,
                [ContentItem::output_text(text)],
            )],
            code_calls: Vec::new(),
            usage: None,
            time_to_first_event_ns: 0,
            time_to_first_output_ns: Some(0),
            pipeline_stats: ResponsePipelineStats::default(),
        })
    }

    fn code_mode_generation(input: &str) -> ResponsesOutput {
        let output_item = serde_json::from_value(json!({
            "type": "custom_tool_call",
            "call_id": "call-native-exec",
            "name": "exec",
            "input": input,
        }))
        .expect("code-mode fixture must decode");
        ResponsesOutput::Generation(GenerationOutput {
            id: "response-native-exec".to_owned(),
            status: "completed".to_owned(),
            end_turn: Some(false),
            final_message: None,
            output_items: vec![output_item],
            code_calls: vec![CodeCall {
                call_id: "call-native-exec".to_owned(),
                name: "exec".to_owned(),
                namespace: None,
                input: input.to_owned(),
                kind: CodeCallKind::Custom,
            }],
            usage: None,
            time_to_first_event_ns: 0,
            time_to_first_output_ns: None,
            pipeline_stats: ResponsePipelineStats::default(),
        })
    }

    fn contains_string(value: &Value, expected: &str) -> bool {
        match value {
            Value::String(value) => value.contains(expected),
            Value::Array(values) => values.iter().any(|value| contains_string(value, expected)),
            Value::Object(values) => values
                .values()
                .any(|value| contains_string(value, expected)),
            Value::Null | Value::Bool(_) | Value::Number(_) => false,
        }
    }

    fn fresh_process_path(name: &str) -> PathBuf {
        PathBuf::from(
            std::env::var_os(name)
                .unwrap_or_else(|| panic!("fresh-process child path is unavailable")),
        )
    }

    fn run_fresh_process_phase(
        phase: &str,
        workspace: &Path,
        snapshot_path: &Path,
        observed: &Path,
    ) {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("fresh-process child runtime must start");
        runtime.block_on(async {
            let is_resume = phase == "resume";
            let snapshot = if is_resume {
                let encoded =
                    std::fs::read(snapshot_path).expect("fresh-process snapshot must be readable");
                Some(
                    serde_json::from_slice::<SessionSnapshot>(&encoded)
                        .expect("fresh-process snapshot must deserialize"),
                )
            } else {
                None
            };
            let observed = observed.to_path_buf();
            let openai = OpenAi::builder("fixture-key")
                .service(move || {
                    let observed = observed.clone();
                    service_fn(move |request: ResponsesAttempt| {
                        let observed = observed.clone();
                        async move {
                            let output = if matches!(request.kind(), ResponsesAttemptKind::Warmup) {
                                ResponsesOutput::Warmup(WarmupResponse {
                                    id: "fresh-process-warmup".to_owned(),
                                    usage: None,
                                })
                            } else {
                                if is_resume {
                                    let input = request
                                        .input_items()
                                        .map(|item| {
                                            serde_json::to_value(item)
                                                .expect("request item must serialize")
                                        })
                                        .collect::<Vec<_>>();
                                    std::fs::write(
                                        observed,
                                        serde_json::to_vec(&input)
                                            .expect("observed request must serialize"),
                                    )
                                    .expect("observed request must be persisted");
                                }
                                deterministic_generation(
                                    "fresh-process-response",
                                    "fresh-process-answer",
                                )
                            };
                            Ok::<_, ResponseError>(ResponsesServiceResponse::new(output))
                        }
                    })
                })
                .build()
                .expect("fresh-process OpenAI fixture must build");
            let (notice_tx, _notice_rx) = mpsc::channel(256);
            let prompt = if is_resume {
                "fresh-process-second-prompt"
            } else {
                "fresh-process-first-prompt"
            };
            let outcome = run_with_openai(
                validated_request(workspace, prompt, snapshot),
                openai,
                notice_tx,
                CancellationToken::new(),
            )
            .await;
            let completed = match outcome {
                BackendOutcome::Completed(completed) => completed,
                BackendOutcome::Cancelled { .. } | BackendOutcome::Failed { .. } => {
                    panic!("fresh-process deterministic turn did not complete")
                }
            };
            if phase == "create" {
                let snapshot: SessionSnapshot = serde_json::from_value(completed.snapshot)
                    .expect("completed snapshot must deserialize");
                std::fs::write(
                    snapshot_path,
                    serde_json::to_vec(&snapshot).expect("typed snapshot must serialize"),
                )
                .expect("typed snapshot must be persisted");
            }
        });
    }

    fn spawn_fresh_process_phase(phase: &str, workspace: &Path, snapshot: &Path, observed: &Path) {
        let status = Command::new(std::env::current_exe().expect("test executable must resolve"))
            .args(["--exact", FRESH_PROCESS_TEST])
            .env_clear()
            .env(FRESH_PROCESS_GUARD_ENV, FRESH_PROCESS_GUARD)
            .env(FRESH_PROCESS_PHASE_ENV, phase)
            .env(FRESH_PROCESS_WORKSPACE_ENV, workspace)
            .env(FRESH_PROCESS_SNAPSHOT_ENV, snapshot)
            .env(FRESH_PROCESS_OBSERVED_ENV, observed)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .expect("fresh-process child must launch");
        assert!(status.success(), "fresh-process child phase failed");
    }

    fn run_tool_environment_child(workspace: &Path) {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("tool-environment child runtime must start");
        runtime.block_on(async {
            let auth_config = AuthConfig::ApiKeyEnv {
                environment_variable: CUSTOM_API_KEY_ENV.to_owned(),
            };
            let auth = resolve_auth(&auth_config).expect("child API key must load");
            let generation = Arc::new(AtomicUsize::new(0));
            let openai = OpenAi::builder(auth)
                .service(move || {
                    let generation = Arc::clone(&generation);
                    service_fn(move |request: ResponsesAttempt| {
                        let generation = Arc::clone(&generation);
                        async move {
                            let output = if matches!(request.kind(), ResponsesAttemptKind::Warmup) {
                                ResponsesOutput::Warmup(WarmupResponse {
                                    id: "tool-environment-warmup".to_owned(),
                                    usage: None,
                                })
                            } else if generation.fetch_add(1, Ordering::SeqCst) == 0 {
                                code_mode_generation(
                                    r#"const result = await tools.exec_command({
  cmd: "if [ -z \"$SMITHERS_PROVIDER_VALUE\" ] && [ \"$SMITHERS_UNRELATED_VALUE\" = \"preserved\" ]; then printf sanitized > native-tool-env.txt; else printf exposed > native-tool-env.txt; fi",
  shell: "/bin/sh",
  login: false
});
text(result.output);"#,
                                )
                            } else {
                                deterministic_generation(
                                    "tool-environment-response",
                                    "tool environment checked",
                                )
                            };
                            Ok::<_, ResponseError>(ResponsesServiceResponse::new(output))
                        }
                    })
                })
                .build()
                .expect("tool-environment fixture must build");
            let mut request = validated_request(workspace, "check the native tool environment", None);
            request.auth = auth_config.clone();
            request.tools = tools_for_auth(&auth_config).expect("stock tools must configure");
            let (notice_tx, _notice_rx) = mpsc::channel(256);
            let outcome = run_with_openai(
                request,
                openai,
                notice_tx,
                CancellationToken::new(),
            )
            .await;
            assert!(matches!(outcome, BackendOutcome::Completed(_)));
        });
    }

    #[cfg(target_os = "linux")]
    #[derive(Clone, Copy)]
    struct LinuxProcessIdentity {
        pid: i32,
        parent_pid: i32,
        process_group: i32,
        state: char,
        start_time: u64,
    }

    #[cfg(target_os = "linux")]
    fn linux_process_identity(pid: i32) -> Option<LinuxProcessIdentity> {
        let stat = std::fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
        let (_, fields) = stat.rsplit_once(") ")?;
        let fields = fields.split_whitespace().collect::<Vec<_>>();
        Some(LinuxProcessIdentity {
            pid,
            state: fields.first()?.chars().next()?,
            parent_pid: fields.get(1)?.parse().ok()?,
            process_group: fields.get(2)?.parse().ok()?,
            start_time: fields.get(19)?.parse().ok()?,
        })
    }

    #[cfg(target_os = "linux")]
    fn same_process_survives(process: LinuxProcessIdentity) -> bool {
        linux_process_identity(process.pid).is_some_and(|current| {
            current.start_time == process.start_time && !matches!(current.state, 'Z' | 'X')
        })
    }

    #[cfg(target_os = "linux")]
    fn read_pid(path: &Path) -> Option<i32> {
        std::fs::read_to_string(path).ok()?.parse().ok()
    }

    #[cfg(target_os = "linux")]
    async fn wait_for_native_process_tree(
        workspace: &Path,
    ) -> (LinuxProcessIdentity, LinuxProcessIdentity) {
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let shell =
                    read_pid(&workspace.join("native-shell.pid")).and_then(linux_process_identity);
                let descendant = read_pid(&workspace.join("native-descendant.pid"))
                    .and_then(linux_process_identity);
                if let (Some(shell), Some(descendant)) = (shell, descendant)
                    && descendant.parent_pid == shell.pid
                    && shell.process_group == shell.pid
                    && descendant.process_group == shell.pid
                    && same_process_survives(shell)
                    && same_process_survives(descendant)
                {
                    return (shell, descendant);
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("native tool process tree did not start")
    }

    #[test]
    fn environment_names_are_restricted() {
        assert!(valid_environment_name("OPENAI_API_KEY"));
        assert!(valid_environment_name("_TEST1"));
        assert!(!valid_environment_name(""));
        assert!(!valid_environment_name("1KEY"));
        assert!(!valid_environment_name("KEY-NAME"));
    }

    #[test]
    fn configured_api_key_env_is_empty_in_native_tool_process() {
        if std::env::var(TOOL_ENV_GUARD_ENV).as_deref() == Ok(TOOL_ENV_GUARD) {
            run_tool_environment_child(&fresh_process_path(TOOL_ENV_WORKSPACE_ENV));
            return;
        }

        let workspace = tempfile::tempdir().expect("tool-environment workspace must exist");
        let status = Command::new(std::env::current_exe().expect("test executable must resolve"))
            .args(["--exact", TOOL_ENV_TEST])
            .env_clear()
            .env(TOOL_ENV_GUARD_ENV, TOOL_ENV_GUARD)
            .env(TOOL_ENV_WORKSPACE_ENV, workspace.path())
            .env(CUSTOM_API_KEY_ENV, "api-key-secret-sentinel")
            .env(UNRELATED_TOOL_ENV, "preserved")
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .status()
            .expect("tool-environment child must launch");
        assert!(status.success(), "tool-environment child failed");
        assert_eq!(
            std::fs::read_to_string(workspace.path().join("native-tool-env.txt")).unwrap(),
            "sanitized"
        );
    }

    #[test]
    fn api_key_environment_override_keeps_all_stock_tool_families() {
        let tools = tools_for_auth(&AuthConfig::ApiKeyEnv {
            environment_variable: "AN_ARBITRARY_NAME".to_owned(),
        })
        .unwrap();
        assert!(tools.workspace_enabled());
        assert!(tools.web_search_enabled());
        assert!(tools.image_generation_enabled());
    }

    #[test]
    fn empty_codex_home_falls_back_to_the_upstream_default_home() {
        assert_eq!(
            default_chatgpt_auth_file_from(
                None,
                Some(OsString::new()),
                Some(OsString::from("/account")),
                Some(OsString::from("/profile")),
            ),
            Some(PathBuf::from("/account/.codex/auth.json"))
        );
        assert_eq!(
            default_chatgpt_auth_file_from(
                None,
                Some(OsString::from("/state")),
                Some(OsString::from("/account")),
                None,
            ),
            Some(PathBuf::from("/state/auth.json"))
        );
    }

    #[test]
    fn managed_auth_preflight_accepts_only_bounded_regular_files() {
        let root = tempfile::tempdir().unwrap();
        let regular = root.path().join("regular.json");
        std::fs::write(&regular, b"{}").unwrap();
        validate_managed_auth_file(&regular).unwrap();

        let directory = root.path().join("directory");
        std::fs::create_dir(&directory).unwrap();
        let error = validate_managed_auth_file(&directory).unwrap_err();
        assert_eq!(error.code, "invalid_auth_file_type");
        assert_eq!(error.category, ErrorCategory::Auth);

        let oversized = root.path().join("oversized.json");
        let file = std::fs::File::create(&oversized).unwrap();
        file.set_len((MAX_MANAGED_AUTH_FILE_BYTES + 1) as u64)
            .unwrap();
        let error = validate_managed_auth_file(&oversized).unwrap_err();
        assert_eq!(error.code, "auth_file_too_large");
        assert_eq!(error.retry, RetryDisposition::Never);

        let error = validate_managed_auth_file(&root.path().join("missing.json")).unwrap_err();
        assert_eq!(error.code, "auth_file_unavailable");
    }

    #[test]
    fn managed_auth_streaming_limit_does_not_trust_advertised_length() {
        let virtual_file = std::io::Cursor::new(vec![0_u8; MAX_MANAGED_AUTH_FILE_BYTES + 1]);
        assert!(matches!(
            read_bounded_managed_auth(virtual_file, 0),
            Err(ManagedAuthFileError::TooLarge)
        ));

        let boundary = std::io::Cursor::new(vec![0_u8; MAX_MANAGED_AUTH_FILE_BYTES]);
        assert_eq!(
            read_bounded_managed_auth(boundary, 0).unwrap().len(),
            MAX_MANAGED_AUTH_FILE_BYTES
        );
    }

    #[test]
    fn managed_auth_staging_persists_refresh_and_preserves_external_updates() {
        let root = tempfile::tempdir().unwrap();
        let original = root.path().join("original.json");
        let staged = root.path().join("staged.json");
        std::fs::write(&original, b"initial").unwrap();
        std::fs::write(&staged, b"initial").unwrap();
        let mut store = BoundedManagedAuthStore {
            original: original.clone(),
            staged: staged.clone(),
            baseline: b"initial".to_vec(),
        };

        std::fs::write(&staged, b"refreshed").unwrap();
        assert_eq!(
            store.sync_after_inner().unwrap(),
            AuthSyncDisposition::UpdatedOriginal
        );
        assert_eq!(std::fs::read(&original).unwrap(), b"refreshed");

        std::fs::write(&original, b"external").unwrap();
        assert_eq!(
            store.sync_from_original().unwrap(),
            AuthSyncDisposition::AdoptedExternal
        );
        assert_eq!(std::fs::read(&staged).unwrap(), b"external");

        std::fs::write(&staged, b"local-race").unwrap();
        std::fs::write(&original, b"external-race").unwrap();
        assert_eq!(
            store.sync_after_inner().unwrap(),
            AuthSyncDisposition::AdoptedExternal
        );
        assert_eq!(std::fs::read(&staged).unwrap(), b"external-race");

        assert!(
            !atomic_write_managed_auth_if_unchanged(
                &original,
                b"stale-generation",
                b"unobserved-refresh",
            )
            .unwrap()
        );
        assert_eq!(std::fs::read(&original).unwrap(), b"external-race");

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;

            assert_eq!(
                std::fs::metadata(&staged).unwrap().permissions().mode() & 0o777,
                0o600
            );
        }
    }

    fn managed_auth_document(account_id: &str, access_token: &str) -> Vec<u8> {
        use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};

        let claims = json!({
            "email": "fixture@example.com",
            "https://api.openai.com/auth": {
                "chatgpt_account_id": account_id,
                "chatgpt_plan_type": "plus",
            },
        });
        let id_token = format!(
            "header.{}.signature",
            URL_SAFE_NO_PAD.encode(serde_json::to_vec(&claims).unwrap())
        );
        serde_json::to_vec(&json!({
            "auth_mode": "chatgpt",
            "tokens": {
                "id_token": id_token,
                "access_token": access_token,
                "refresh_token": format!("refresh-{access_token}"),
                "account_id": account_id,
            },
            "last_refresh": "2026-07-29T00:00:00Z",
        }))
        .unwrap()
    }

    #[tokio::test]
    async fn managed_auth_source_adopts_external_bearer_and_rejects_account_change() {
        let root = tempfile::tempdir().unwrap();
        let original = root.path().join("auth.json");
        std::fs::write(&original, managed_auth_document("account-1", "access-old")).unwrap();
        let auth = load_bounded_chatgpt_auth(&original).unwrap();
        assert_eq!(auth.snapshot().await.unwrap().bearer(), "access-old");

        std::fs::write(&original, managed_auth_document("account-1", "access-new")).unwrap();
        assert_eq!(auth.snapshot().await.unwrap().bearer(), "access-new");

        std::fs::write(
            &original,
            managed_auth_document("account-2", "access-other-account"),
        )
        .unwrap();
        assert!(matches!(
            auth.snapshot().await,
            Err(OpenAiAuthError::AccountChanged)
        ));
        assert!(matches!(
            auth.snapshot().await,
            Err(OpenAiAuthError::AccountChanged)
        ));

        std::fs::write(&original, b"not-json").unwrap();
        assert!(matches!(
            auth.snapshot().await,
            Err(OpenAiAuthError::Unavailable(_))
        ));
        assert!(matches!(
            auth.snapshot().await,
            Err(OpenAiAuthError::Unavailable(_))
        ));

        std::fs::write(
            &original,
            managed_auth_document("account-1", "access-recovered"),
        )
        .unwrap();
        assert_eq!(auth.snapshot().await.unwrap().bearer(), "access-recovered");
    }

    #[test]
    fn emitted_snapshots_are_rejected_at_every_structural_limit() {
        let context = ResumeEnvelopeContext {
            workspace: "/workspace".to_owned(),
            auth: json!({
                "mode": "api-key-env",
                "environmentVariable": "OPENAI_API_KEY",
            }),
            options: maximal_resume_options(&TurnOptions::default()),
        };
        fn assert_rejected(snapshot: Value) {
            let context = ResumeEnvelopeContext {
                workspace: "/workspace".to_owned(),
                auth: json!({
                    "mode": "api-key-env",
                    "environmentVariable": "OPENAI_API_KEY",
                }),
                options: maximal_resume_options(&TurnOptions::default()),
            };
            let error = validate_completed_snapshot_resumability(&snapshot, &context).unwrap_err();
            assert_eq!(error.code, "snapshot_structure_too_large");
            assert_eq!(error.category, ErrorCategory::Checkpoint);
            assert_eq!(error.retry, RetryDisposition::Never);
        }

        {
            let mut nested = Value::Null;
            for _ in 0..61 {
                nested = Value::Array(vec![nested]);
            }
            crate::strict_json::validate_value(&nested).unwrap();
            let error = validate_completed_snapshot_resumability(&nested, &context).unwrap_err();
            assert_eq!(error.code, "snapshot_structure_too_large");
        }
        {
            let inner_len = (crate::strict_json::MAX_JSON_NODES - 4) / 2;
            let snapshot = json!({
                "history": [
                    vec![Value::Null; inner_len],
                    vec![Value::Null; inner_len],
                ]
            });
            crate::strict_json::validate_value(&snapshot).unwrap();
            let error = validate_completed_snapshot_resumability(&snapshot, &context).unwrap_err();
            assert_eq!(error.code, "snapshot_structure_too_large");
        }
        {
            let mut members = serde_json::Map::new();
            for index in 0..=crate::strict_json::MAX_JSON_OBJECT_MEMBERS {
                members.insert(format!("field{index}"), Value::Null);
            }
            assert_rejected(Value::Object(members));
        }
        assert_rejected(json!({
            "history": vec![Value::Null; crate::strict_json::MAX_JSON_ARRAY_ELEMENTS + 1]
        }));
        {
            let mut members = serde_json::Map::new();
            members.insert(
                "x".repeat(crate::strict_json::MAX_JSON_KEY_BYTES + 1),
                Value::Null,
            );
            assert_rejected(Value::Object(members));
        }
        assert_rejected(json!({
            "value": "x".repeat(crate::strict_json::MAX_JSON_STRING_BYTES + 1)
        }));
    }

    #[test]
    fn emitted_snapshot_must_fit_a_minimal_resume_record() {
        let options = TurnOptions {
            instructions: Some("\u{1}".repeat(3 * 1024 * 1024)),
            ..TurnOptions::default()
        };
        let context = ResumeEnvelopeContext {
            workspace: "/workspace".to_owned(),
            auth: json!({
                "mode": "api-key-env",
                "environmentVariable": "OPENAI_API_KEY",
            }),
            options: maximal_resume_options(&options),
        };
        let snapshot = json!({"history": "x".repeat(7 * 1024 * 1024)});
        assert!(serde_json::to_vec(&snapshot).unwrap().len() < MAX_SNAPSHOT_BYTES);
        crate::strict_json::validate_value(&snapshot).unwrap();

        let error = validate_completed_snapshot_resumability(&snapshot, &context).unwrap_err();
        assert_eq!(error.code, "snapshot_resume_record_too_large");
        assert_eq!(error.category, ErrorCategory::Checkpoint);
    }

    #[cfg(unix)]
    #[test]
    fn managed_auth_symlinks_follow_only_bounded_regular_targets() {
        use std::{
            ffi::CString,
            os::{unix::ffi::OsStrExt, unix::fs::symlink},
            sync::mpsc as std_mpsc,
        };

        let root = tempfile::tempdir().unwrap();
        let regular = root.path().join("regular.json");
        std::fs::write(&regular, b"{}").unwrap();
        let regular_link = root.path().join("regular-link.json");
        symlink(&regular, &regular_link).unwrap();
        validate_managed_auth_file(&regular_link).unwrap();

        let fifo = root.path().join("auth.fifo");
        let fifo_name = CString::new(fifo.as_os_str().as_bytes()).unwrap();
        assert_eq!(unsafe { libc::mkfifo(fifo_name.as_ptr(), 0o600) }, 0);
        let fifo_link = root.path().join("fifo-link.json");
        symlink(&fifo, &fifo_link).unwrap();

        let (sender, receiver) = std_mpsc::channel();
        std::thread::spawn(move || sender.send(validate_managed_auth_file(&fifo_link)).unwrap());
        let error = receiver
            .recv_timeout(Duration::from_secs(1))
            .expect("FIFO metadata validation must not block")
            .unwrap_err();
        assert_eq!(error.code, "invalid_auth_file_type");

        let device = validate_managed_auth_file(Path::new("/dev/null")).unwrap_err();
        assert_eq!(device.code, "invalid_auth_file_type");

        let broken_link = root.path().join("broken-link.json");
        symlink(root.path().join("absent.json"), &broken_link).unwrap();
        assert_eq!(
            validate_managed_auth_file(&broken_link).unwrap_err().code,
            "auth_file_unavailable"
        );
    }

    #[test]
    fn managed_auth_parser_failures_have_specific_public_taxonomy() {
        let root = tempfile::tempdir().unwrap();
        let invalid = root.path().join("invalid.json");
        std::fs::write(&invalid, b"not-json").unwrap();
        let error = resolve_auth(&AuthConfig::Chatgpt {
            auth_file: Some(invalid),
        })
        .unwrap_err();
        assert_eq!(error.code, "invalid_auth_file");
        assert_eq!(error.category, ErrorCategory::Auth);
        assert_eq!(error.retry, RetryDisposition::Never);
    }

    #[test]
    fn pinned_upstream_provider_errors_keep_auth_checkpoint_and_retry_classes() {
        let checkpoint = NanocodexError::Response(ResponseError::from(ResponsesError::Api {
            event: r#"{"error":{"code":"previous_response_not_found"}}"#.to_owned(),
        }));
        let checkpoint = map_nanocodex_error(&checkpoint);
        assert_eq!(checkpoint.code, "checkpoint_missing");
        assert_eq!(checkpoint.category, ErrorCategory::Checkpoint);
        assert_eq!(checkpoint.retry, RetryDisposition::Never);

        let auth = NanocodexError::Response(ResponseError::from(ResponsesError::Authorization {
            detail: "credential source unavailable".to_owned(),
        }));
        let auth = map_nanocodex_error(&auth);
        assert_eq!(auth.code, "provider_authorization");
        assert_eq!(auth.category, ErrorCategory::Auth);
        assert!(!auth.message.contains("credential source unavailable"));

        let provider =
            NanocodexError::Response(ResponseError::from(ResponsesError::HandshakeRejected {
                status: 429,
                body: "provider-secret-body".to_owned(),
                retry_after: Some(Duration::from_millis(1250)),
            }));
        let provider = map_nanocodex_error(&provider);
        assert_eq!(provider.code, "provider_handshake_rejected");
        assert_eq!(provider.category, ErrorCategory::Provider);
        assert_eq!(provider.retry, RetryDisposition::After);
        assert_eq!(provider.retry_after_ms, Some(1250));
        assert!(!provider.message.contains("provider-secret-body"));

        let handshake_403 =
            NanocodexError::Response(ResponseError::from(ResponsesError::HandshakeRejected {
                status: 403,
                body: "chatgpt-edge-secret".to_owned(),
                retry_after: Some(Duration::from_secs(2)),
            }));
        let handshake_403 = map_nanocodex_error(&handshake_403);
        assert_eq!(handshake_403.code, "provider_handshake_rejected");
        assert_eq!(handshake_403.category, ErrorCategory::Provider);
        assert_eq!(handshake_403.retry, RetryDisposition::Safe);
        assert_eq!(handshake_403.retry_after_ms, None);
        assert!(!handshake_403.message.contains("chatgpt-edge-secret"));

        let overflow =
            NanocodexError::Response(ResponseError::from(ResponsesError::HandshakeRejected {
                status: 429,
                body: "retry-secret".to_owned(),
                retry_after: Some(Duration::MAX),
            }));
        let overflow = map_nanocodex_error(&overflow);
        assert_eq!(overflow.retry, RetryDisposition::Safe);
        assert_eq!(overflow.retry_after_ms, None);
        assert!(!overflow.message.contains("retry-secret"));

        for rejected in [
            ResponsesError::HandshakeRejected {
                status: 401,
                body: "credential-secret-body".to_owned(),
                retry_after: None,
            },
            ResponsesError::HttpRejected {
                status: 403,
                body: "credential-secret-body".to_owned(),
                retry_after: None,
            },
        ] {
            let auth =
                map_nanocodex_error(&NanocodexError::Response(ResponseError::from(rejected)));
            assert_eq!(auth.category, ErrorCategory::Auth);
            assert_eq!(auth.retry, RetryDisposition::Never);
            assert!(!auth.message.contains("credential-secret-body"));
        }
    }

    #[test]
    fn pinned_upstream_auth_and_checkpoint_variants_have_specific_taxonomy() {
        let login = map_openai_auth_error(&OpenAiAuthError::LoginRequired(Arc::from("expired")));
        assert_eq!(login.code, "auth_login_required");
        assert_eq!(login.category, ErrorCategory::Auth);
        assert_eq!(login.retry, RetryDisposition::Never);

        let unavailable = map_openai_auth_error(&OpenAiAuthError::Unavailable(Arc::from("locked")));
        assert_eq!(unavailable.code, "auth_temporarily_unavailable");
        assert_eq!(unavailable.retry, RetryDisposition::Safe);

        let lineage = map_nanocodex_error(&NanocodexError::CheckpointLineageMismatch);
        assert_eq!(lineage.code, "checkpoint_lineage_mismatch");
        assert_eq!(lineage.category, ErrorCategory::Checkpoint);
    }

    #[test]
    fn nanocodex_error_map_is_exhaustive_for_pinned_variants() {
        let serde_error = serde_json::from_str::<Value>("").unwrap_err();
        let cases: Vec<(NanocodexError, &str, ErrorCategory, RetryDisposition)> = vec![
            (
                NanocodexError::InvalidRequest("policy".into()),
                "invalid_agent_configuration",
                ErrorCategory::Config,
                RetryDisposition::Never,
            ),
            (
                NanocodexError::ResolveWorkspace {
                    path: PathBuf::from("/missing"),
                    source: std::io::Error::from(ErrorKind::NotFound),
                },
                "workspace_invalid",
                ErrorCategory::Workspace,
                RetryDisposition::Never,
            ),
            (
                NanocodexError::WorkspaceNotDirectory {
                    path: PathBuf::from("/tmp"),
                },
                "workspace_invalid",
                ErrorCategory::Workspace,
                RetryDisposition::Never,
            ),
            (
                NanocodexError::WorkspaceNotUtf8 {
                    path: PathBuf::from("/tmp"),
                },
                "workspace_invalid",
                ErrorCategory::Workspace,
                RetryDisposition::Never,
            ),
            (
                NanocodexError::WorkspaceChanged {
                    current: "/a".into(),
                    requested: "/b".into(),
                },
                "workspace_changed",
                ErrorCategory::Workspace,
                RetryDisposition::Never,
            ),
            (
                NanocodexError::MalformedResponse { detail: "shape" },
                "provider_protocol_failed",
                ErrorCategory::Provider,
                RetryDisposition::Never,
            ),
            (
                NanocodexError::InvalidAttemptState { detail: "state" },
                "provider_protocol_failed",
                ErrorCategory::Provider,
                RetryDisposition::Never,
            ),
            (
                NanocodexError::SerializePromptPrefix(serde_error),
                "invalid_agent_configuration",
                ErrorCategory::Config,
                RetryDisposition::Never,
            ),
            (
                NanocodexError::AgentStopped,
                "agent_stopped",
                ErrorCategory::Internal,
                RetryDisposition::Safe,
            ),
            (
                NanocodexError::TurnStopped,
                "turn_stopped",
                ErrorCategory::Internal,
                RetryDisposition::Safe,
            ),
            (
                NanocodexError::Shutdown(Arc::new(NanocodexError::TurnStopped)),
                "cleanup_failed",
                ErrorCategory::Cleanup,
                RetryDisposition::Safe,
            ),
            (
                NanocodexError::TurnNotSteerable,
                "steering_unsupported",
                ErrorCategory::Config,
                RetryDisposition::Never,
            ),
            (
                NanocodexError::SteerQueueFull,
                "steering_unsupported",
                ErrorCategory::Config,
                RetryDisposition::Never,
            ),
            (
                NanocodexError::TurnNotCancellable,
                "turn_not_cancellable",
                ErrorCategory::Protocol,
                RetryDisposition::Never,
            ),
            (
                NanocodexError::TurnCancelled,
                "turn_cancelled",
                ErrorCategory::Internal,
                RetryDisposition::Safe,
            ),
            (
                NanocodexError::ForkBeforeCompletedTurn,
                "checkpoint_unavailable",
                ErrorCategory::Checkpoint,
                RetryDisposition::Never,
            ),
            (
                NanocodexError::InvalidSessionSnapshot("bad".into()),
                "invalid_snapshot",
                ErrorCategory::Checkpoint,
                RetryDisposition::Never,
            ),
            (
                NanocodexError::TokioRuntimeUnavailable,
                "invalid_agent_configuration",
                ErrorCategory::Config,
                RetryDisposition::Never,
            ),
            (
                NanocodexError::InitializeRollout {
                    codex_home: PathBuf::from("/tmp"),
                    source: std::io::Error::from(ErrorKind::PermissionDenied),
                },
                "nanocodex_failed",
                ErrorCategory::Internal,
                RetryDisposition::Never,
            ),
            (
                NanocodexError::PersistRollout {
                    path: PathBuf::from("/tmp/rollout"),
                    source: std::io::Error::from(ErrorKind::PermissionDenied),
                },
                "nanocodex_failed",
                ErrorCategory::Internal,
                RetryDisposition::Never,
            ),
            (
                NanocodexError::Event(EventError::ClosedBeforeTerminal),
                "event_protocol_failed",
                ErrorCategory::Internal,
                RetryDisposition::Safe,
            ),
            (
                NanocodexError::Tools(nanocodex::tools::ToolsBuildError::EmptyName),
                "invalid_tool_configuration",
                ErrorCategory::Tool,
                RetryDisposition::Never,
            ),
        ];
        for (error, code, category, retry) in cases {
            let public = map_nanocodex_error(&error);
            assert_eq!(public.code, code, "{error:?}");
            assert_eq!(public.category, category, "{error:?}");
            assert_eq!(public.retry, retry, "{error:?}");
        }
    }

    fn turn_start_at(workspace: PathBuf) -> TurnStartData {
        TurnStartData {
            prompt: "hello".to_owned(),
            workspace,
            auth: AuthConfig::ApiKeyEnv {
                environment_variable: "UNUSED_TEST_KEY".to_owned(),
            },
            transport: crate::protocol::TransportConfig::Websocket,
            options: crate::protocol::TurnOptions::default(),
            continuation: None,
        }
    }

    #[test]
    fn validate_request_rejects_relative_missing_and_non_directory_workspaces() {
        let relative = validate_request(turn_start_at(PathBuf::from("relative")))
            .err()
            .expect("relative workspace must be rejected");
        assert_eq!(relative.code, "workspace_not_absolute");
        assert_eq!(relative.category, ErrorCategory::Workspace);

        let missing = validate_request(turn_start_at(
            std::env::temp_dir().join("smithers-nanocodex-missing-workspace"),
        ))
        .err()
        .expect("missing workspace must be rejected");
        assert_eq!(missing.code, "workspace_unavailable");

        let file = tempfile::NamedTempFile::new().unwrap();
        let not_dir = validate_request(turn_start_at(file.path().canonicalize().unwrap()))
            .err()
            .expect("non-directory workspace must be rejected");
        assert_eq!(not_dir.code, "workspace_not_directory");
    }

    #[test]
    fn validate_request_rejects_empty_and_oversized_prompts_and_instructions() {
        let mut empty = turn_start_at(PathBuf::from("/unused"));
        empty.prompt.clear();
        assert_eq!(
            validate_request(empty)
                .err()
                .expect("empty prompt must be rejected")
                .code,
            "invalid_prompt"
        );

        let mut oversized = turn_start_at(PathBuf::from("/unused"));
        oversized.prompt = "x".repeat(MAX_PROMPT_BYTES + 1);
        assert_eq!(
            validate_request(oversized)
                .err()
                .expect("oversized prompt must be rejected")
                .code,
            "invalid_prompt"
        );

        let workspace = tempfile::tempdir().unwrap();
        let mut empty_instructions = turn_start_at(workspace.path().to_path_buf());
        empty_instructions.options.instructions = Some("   ".to_owned());
        assert_eq!(
            validate_request(empty_instructions)
                .err()
                .expect("empty instructions must be rejected")
                .code,
            "invalid_instructions"
        );

        let mut oversized_instructions = turn_start_at(workspace.path().to_path_buf());
        oversized_instructions.options.instructions = Some("x".repeat(MAX_PROMPT_BYTES + 1));
        assert_eq!(
            validate_request(oversized_instructions)
                .err()
                .expect("oversized instructions must be rejected")
                .code,
            "invalid_instructions"
        );
    }

    #[tokio::test]
    async fn event_queue_backpressure_is_reported_with_a_coalesced_marker() {
        let (notice_tx, mut notice_rx) = mpsc::channel(1);
        notice_tx
            .send(BackendNotice::Accepted {
                session_id: "occupied".to_owned(),
                cancellation: Arc::new(NoopCancellation),
            })
            .await
            .unwrap();
        let event: nanocodex::oai::events::AgentEvent = serde_json::from_value(json!({
            "protocol_version": 1,
            "request_id": "upstream-request",
            "seq": 1,
            "type": "assistant.delta",
            "payload": {
                "model_call_index": 0,
                "item_id": null,
                "phase": null,
                "text": "hello"
            }
        }))
        .unwrap();
        let mut forwarder = EventForwarder::new(notice_tx);
        forwarder.forward(&event).unwrap();
        assert!(matches!(
            notice_rx.recv().await,
            Some(BackendNotice::Accepted { .. })
        ));

        forwarder.forward(&event).unwrap();
        assert!(matches!(
            notice_rx.recv().await,
            Some(BackendNotice::EventTruncated {
                reason: "bridge_event_backpressure",
                ..
            })
        ));
        forwarder.finish().await.unwrap();
        assert!(matches!(
            notice_rx.recv().await,
            Some(BackendNotice::EventTruncated {
                reason: "bridge_event_backpressure",
                ..
            })
        ));
    }

    #[tokio::test]
    async fn provider_frames_and_tool_bodies_never_cross_the_event_boundary() {
        let secret = "provider-prompt-secret-sentinel";
        let provider_event: AgentEvent = serde_json::from_value(json!({
            "protocol_version": 1,
            "request_id": "upstream-request",
            "seq": 7,
            "type": "api.event",
            "payload": {
                "direction": "outbound",
                "transport": "websocket",
                "phase": "generation",
                "model_call_index": 0,
                "event": {"input": secret}
            }
        }))
        .unwrap();
        let (notice_tx, mut notice_rx) = mpsc::channel(8);
        let mut forwarder = EventForwarder::new(notice_tx);
        forwarder.forward(&provider_event).unwrap();
        assert!(matches!(
            notice_rx.recv().await,
            Some(BackendNotice::EventTruncated {
                reason: "event_policy",
                upstream_seq: Some(7),
                ..
            })
        ));

        let tool_event: AgentEvent = serde_json::from_value(json!({
            "protocol_version": 1,
            "request_id": "upstream-request",
            "seq": 8,
            "type": "tool.call",
            "payload": {
                "call_id": "call-1",
                "tool": "exec_command",
                "arguments": {"command": secret},
                "model_call_index": 1
            }
        }))
        .unwrap();
        forwarder.forward(&tool_event).unwrap();
        let Some(BackendNotice::Event { event }) = notice_rx.recv().await else {
            panic!("expected a projected tool event");
        };
        let encoded = serde_json::to_string(&event).unwrap();
        assert!(!encoded.contains(secret));
        assert_eq!(event["upstreamSeq"], 8);
        assert_eq!(event["payload"]["callId"], "call-1");
        assert!(event["payload"].get("arguments").is_none());
    }

    #[tokio::test]
    async fn aggregate_event_budget_emits_one_truncation_and_then_drops() {
        let event: AgentEvent = serde_json::from_value(json!({
            "protocol_version": 1,
            "request_id": "upstream-request",
            "seq": 1,
            "type": "api.event",
            "payload": {"direction": "outbound"}
        }))
        .unwrap();
        let (notice_tx, mut notice_rx) = mpsc::channel(8);
        let mut forwarder = EventForwarder::new(notice_tx);
        let mut aggregate_markers = 0usize;
        for _ in 0..(MAX_EVENT_TOTAL_BYTES / EVENT_WIRE_ENVELOPE_RESERVE + 4) {
            forwarder.forward(&event).unwrap();
            while let Ok(notice) = notice_rx.try_recv() {
                match notice {
                    BackendNotice::EventTruncated {
                        reason: "aggregate_event_limit",
                        ..
                    } => aggregate_markers += 1,
                    BackendNotice::EventTruncated {
                        reason: "event_policy",
                        ..
                    } => {}
                    BackendNotice::EventTruncated { reason, .. } => {
                        panic!("unexpected truncation {reason}")
                    }
                    BackendNotice::Event { .. } | BackendNotice::Accepted { .. } => {
                        panic!("unexpected event or acceptance notice")
                    }
                }
            }
        }
        assert_eq!(
            aggregate_markers, 1,
            "aggregate_event_limit must be reported exactly once"
        );
        forwarder.forward(&event).unwrap();
        forwarder.finish().await.unwrap();
        assert!(notice_rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn oversized_raw_event_is_rejected_before_typed_payload_decoding() {
        let oversized = "x".repeat(MAX_EVENT_BYTES);
        let event: AgentEvent = serde_json::from_value(json!({
            "protocol_version": 1,
            "request_id": "upstream-request",
            "seq": 9,
            "type": "assistant.delta",
            "payload": {"untypedOversizedData": oversized}
        }))
        .unwrap();
        let (notice_tx, mut notice_rx) = mpsc::channel(2);
        let mut forwarder = EventForwarder::new(notice_tx);
        forwarder.forward(&event).unwrap();
        assert!(matches!(
            notice_rx.recv().await,
            Some(BackendNotice::EventTruncated {
                reason: "event_limit",
                upstream_seq: Some(9),
                ..
            })
        ));
    }

    #[test]
    fn session_snapshot_resumes_across_fresh_os_processes() {
        if std::env::var(FRESH_PROCESS_GUARD_ENV).as_deref() == Ok(FRESH_PROCESS_GUARD) {
            let phase = std::env::var(FRESH_PROCESS_PHASE_ENV)
                .expect("fresh-process child phase must be declared");
            assert!(
                matches!(phase.as_str(), "create" | "resume"),
                "fresh-process child phase is invalid"
            );
            run_fresh_process_phase(
                &phase,
                &fresh_process_path(FRESH_PROCESS_WORKSPACE_ENV),
                &fresh_process_path(FRESH_PROCESS_SNAPSHOT_ENV),
                &fresh_process_path(FRESH_PROCESS_OBSERVED_ENV),
            );
            return;
        }

        let root = tempfile::tempdir().expect("fresh-process test root must be created");
        let workspace = root.path().join("workspace");
        std::fs::create_dir(&workspace).expect("fresh-process workspace must be created");
        let workspace = workspace
            .canonicalize()
            .expect("fresh-process workspace must canonicalize");
        let snapshot = root.path().join("session-snapshot.json");
        let observed = root.path().join("resumed-request.json");

        spawn_fresh_process_phase("create", &workspace, &snapshot, &observed);
        assert!(
            snapshot.is_file(),
            "snapshot child omitted its durable output"
        );
        spawn_fresh_process_phase("resume", &workspace, &snapshot, &observed);

        let resumed_request: Vec<Value> = serde_json::from_slice(
            &std::fs::read(observed).expect("resumed request must be readable"),
        )
        .expect("resumed request must deserialize");
        assert!(
            resumed_request
                .iter()
                .any(|item| contains_string(item, "fresh-process-first-prompt")),
            "fresh-process resumed request omitted first-turn history"
        );
        assert!(
            resumed_request
                .iter()
                .any(|item| contains_string(item, "fresh-process-answer")),
            "fresh-process resumed request omitted the first assistant response"
        );
        assert!(
            resumed_request
                .iter()
                .any(|item| contains_string(item, "fresh-process-second-prompt")),
            "fresh-process resumed request omitted its new prompt"
        );
    }

    #[tokio::test]
    async fn deterministic_nanocodex_turn_resume_replays_history_in_second_request() {
        let workspace = tempfile::tempdir().unwrap();
        let observed = Arc::new(Mutex::new(Vec::new()));
        let make_openai = || {
            let observed = Arc::clone(&observed);
            OpenAi::builder("fixture-key")
                .service(move || {
                    let observed = Arc::clone(&observed);
                    service_fn(move |request: ResponsesAttempt| {
                        let observed = Arc::clone(&observed);
                        async move {
                            let output = if matches!(request.kind(), ResponsesAttemptKind::Warmup) {
                                ResponsesOutput::Warmup(WarmupResponse {
                                    id: "warmup-fixture".to_owned(),
                                    usage: None,
                                })
                            } else {
                                let input = request
                                    .input_items()
                                    .map(|item| serde_json::to_value(item).unwrap())
                                    .collect::<Vec<_>>();
                                observed.lock().unwrap().push(json!(input));
                                deterministic_generation("response-fixture", "deterministic answer")
                            };
                            Ok::<_, ResponseError>(ResponsesServiceResponse::new(output))
                        }
                    })
                })
                .build()
                .unwrap()
        };

        let (notice_tx, mut notice_rx) = mpsc::channel(256);
        let first = run_with_openai(
            validated_request(workspace.path(), "first-turn-sentinel", None),
            make_openai(),
            notice_tx,
            CancellationToken::new(),
        )
        .await;
        let completed = match first {
            BackendOutcome::Completed(completed) => completed,
            BackendOutcome::Cancelled { .. } | BackendOutcome::Failed { .. } => {
                panic!("first deterministic turn did not complete")
            }
        };
        assert_eq!(completed.final_message, "deterministic answer");
        assert_eq!(completed.snapshot_version, SNAPSHOT_VERSION);
        assert_eq!(completed.usage["inputTokens"], 0);
        assert_eq!(completed.usage["costStatus"], "usage_not_reported");
        assert!(completed.usage.get("input_tokens").is_none());
        assert_eq!(
            std::path::Path::new(&completed.canonical_workspace),
            workspace.path().canonicalize().unwrap()
        );
        assert!(matches!(
            notice_rx.recv().await,
            Some(BackendNotice::Accepted { .. })
        ));
        assert_eq!(
            observed.lock().unwrap().len(),
            1,
            "first turn must issue exactly one generation request"
        );

        let mut non_canonical_snapshot = completed.snapshot.clone();
        non_canonical_snapshot
            .as_object_mut()
            .unwrap()
            .insert("unsupportedField".to_owned(), Value::Bool(true));
        let rejected = validate_request(TurnStartData {
            prompt: "resume".to_owned(),
            workspace: workspace.path().canonicalize().unwrap(),
            auth: AuthConfig::ApiKeyEnv {
                environment_variable: "UNUSED_TEST_KEY".to_owned(),
            },
            transport: crate::protocol::TransportConfig::Websocket,
            options: crate::protocol::TurnOptions::default(),
            continuation: Some(Continuation::Resume {
                snapshot: non_canonical_snapshot,
            }),
        })
        .err()
        .expect("non-canonical snapshot must be rejected");
        assert_eq!(rejected.code, "invalid_snapshot");

        let mut prefixless_snapshot = completed.snapshot.clone();
        prefixless_snapshot
            .as_object_mut()
            .unwrap()
            .remove("request_prefix");
        let rejected = validate_request(TurnStartData {
            prompt: "resume".to_owned(),
            workspace: workspace.path().canonicalize().unwrap(),
            auth: AuthConfig::ApiKeyEnv {
                environment_variable: "UNUSED_TEST_KEY".to_owned(),
            },
            transport: crate::protocol::TransportConfig::Websocket,
            options: crate::protocol::TurnOptions::default(),
            continuation: Some(Continuation::Resume {
                snapshot: prefixless_snapshot,
            }),
        })
        .err()
        .expect("legacy prefix-less snapshot must be rejected");
        assert_eq!(rejected.code, "invalid_snapshot");

        let other = tempfile::tempdir().unwrap();
        let mismatched = validate_request(TurnStartData {
            prompt: "resume".to_owned(),
            workspace: other.path().canonicalize().unwrap(),
            auth: AuthConfig::ApiKeyEnv {
                environment_variable: "UNUSED_TEST_KEY".to_owned(),
            },
            transport: crate::protocol::TransportConfig::Websocket,
            options: crate::protocol::TurnOptions::default(),
            continuation: Some(Continuation::Resume {
                snapshot: completed.snapshot.clone(),
            }),
        })
        .err()
        .expect("workspace-mismatched snapshot must be rejected");
        assert_eq!(mismatched.code, "workspace_changed");
        assert_eq!(mismatched.category, ErrorCategory::Workspace);

        assert_eq!(completed.snapshot["model"], ModelId::WIRE_SOL);
        let mismatched_model = validate_request(TurnStartData {
            prompt: "resume".to_owned(),
            workspace: workspace.path().canonicalize().unwrap(),
            auth: AuthConfig::ApiKeyEnv {
                environment_variable: "UNUSED_TEST_KEY".to_owned(),
            },
            transport: crate::protocol::TransportConfig::Websocket,
            options: crate::protocol::TurnOptions {
                model: Some(ModelId::Luna),
                ..crate::protocol::TurnOptions::default()
            },
            continuation: Some(Continuation::Resume {
                snapshot: completed.snapshot.clone(),
            }),
        })
        .err()
        .expect("explicit mismatched model must be rejected");
        assert_eq!(mismatched_model.code, "model_mismatch");
        assert_eq!(mismatched_model.category, ErrorCategory::Checkpoint);

        let snapshot: SessionSnapshot = serde_json::from_value(completed.snapshot).unwrap();
        let (notice_tx, _notice_rx) = mpsc::channel(256);
        let resumed = run_with_openai(
            validated_request(workspace.path(), "second-turn-sentinel", Some(snapshot)),
            make_openai(),
            notice_tx,
            CancellationToken::new(),
        )
        .await;
        assert!(matches!(resumed, BackendOutcome::Completed(_)));
        let observed = observed.lock().unwrap();
        assert_eq!(
            observed.len(),
            2,
            "resume must issue exactly one additional generation request"
        );
        let resumed_request = &observed[1];
        assert!(
            contains_string(resumed_request, "first-turn-sentinel"),
            "second model request omitted first-turn user history"
        );
        assert!(
            contains_string(resumed_request, "deterministic answer"),
            "second model request omitted first-turn assistant history"
        );
        assert!(
            contains_string(resumed_request, "second-turn-sentinel"),
            "second model request omitted the resumed prompt"
        );
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn exact_turn_control_cancels_native_code_mode_process_tree() {
        let workspace = tempfile::tempdir().expect("native cancellation workspace must exist");
        let input = r#"const result = await tools.exec_command({
  cmd: "printf '%s' \"$$\" > native-shell.pid; sleep 300 & child=\"$!\"; printf '%s' \"$child\" > native-descendant.pid; wait \"$child\"",
  login: false,
  yield_time_ms: 30000
});
text(result.output);"#;
        let openai = OpenAi::builder("fixture-key")
            .service(move || {
                service_fn(move |request: ResponsesAttempt| async move {
                    let output = if matches!(request.kind(), ResponsesAttemptKind::Warmup) {
                        ResponsesOutput::Warmup(WarmupResponse {
                            id: "native-cancellation-warmup".to_owned(),
                            usage: None,
                        })
                    } else {
                        code_mode_generation(input)
                    };
                    Ok::<_, ResponseError>(ResponsesServiceResponse::new(output))
                })
            })
            .build()
            .expect("native cancellation fixture must build");
        let (notice_tx, mut notice_rx) = mpsc::channel(256);
        let task = tokio::spawn(run_with_openai(
            validated_request(workspace.path(), "run native cancellation fixture", None),
            openai,
            notice_tx,
            CancellationToken::new(),
        ));
        let exact_cancellation = tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                if let Some(BackendNotice::Accepted { cancellation, .. }) = notice_rx.recv().await {
                    return cancellation;
                }
            }
        })
        .await
        .expect("turn was not accepted");
        let (shell, descendant) = wait_for_native_process_tree(workspace.path()).await;

        tokio::time::timeout(Duration::from_secs(3), exact_cancellation.cancel())
            .await
            .expect("exact TurnControl cancellation acknowledgement timed out")
            .expect("exact TurnControl cancellation was rejected");
        let outcome = tokio::time::timeout(Duration::from_secs(3), task)
            .await
            .expect("cancelled backend turn did not terminate")
            .expect("cancelled backend task did not join");
        assert!(
            matches!(outcome, BackendOutcome::Cancelled { .. }),
            "native tool cancellation did not produce BackendOutcome::Cancelled"
        );
        tokio::time::timeout(Duration::from_secs(2), async {
            while same_process_survives(shell) || same_process_survives(descendant) {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("native tool process tree survived cancellation");
    }

    #[tokio::test]
    async fn exact_turn_control_cancels_a_blocked_model_call() {
        let workspace = tempfile::tempdir().unwrap();
        let openai = OpenAi::builder("fixture-key")
            .service(|| {
                service_fn(|request: ResponsesAttempt| async move {
                    if matches!(request.kind(), ResponsesAttemptKind::Warmup) {
                        return Ok::<_, ResponseError>(ResponsesServiceResponse::new(
                            ResponsesOutput::Warmup(WarmupResponse {
                                id: "warmup-fixture".to_owned(),
                                usage: None,
                            }),
                        ));
                    }
                    std::future::pending::<()>().await;
                    unreachable!()
                })
            })
            .build()
            .unwrap();
        let (notice_tx, mut notice_rx) = mpsc::channel(256);
        let cancellation = CancellationToken::new();
        let task_cancellation = cancellation.clone();
        let request = validated_request(workspace.path(), "blocked-turn", None);
        let task = tokio::spawn(async move {
            run_with_openai(request, openai, notice_tx, task_cancellation).await
        });
        let exact_cancellation = loop {
            if let Some(BackendNotice::Accepted { cancellation, .. }) = notice_rx.recv().await {
                break cancellation;
            }
        };
        exact_cancellation.cancel().await.unwrap();
        let outcome = tokio::time::timeout(std::time::Duration::from_secs(2), task)
            .await
            .expect("cancellation timed out")
            .unwrap();
        assert!(matches!(outcome, BackendOutcome::Cancelled { .. }));
    }
}
