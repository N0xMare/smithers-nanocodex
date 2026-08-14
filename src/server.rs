use std::{collections::HashSet, io, sync::Arc, time::Duration};

use serde::Serialize;
use serde_json::{Value, json};
use tokio::{
    io::{AsyncBufReadExt, AsyncRead, AsyncWrite, AsyncWriteExt, BufReader},
    sync::{OwnedSemaphorePermit, Semaphore, mpsc},
    task::JoinHandle,
};
use tokio_util::sync::CancellationToken;

use crate::{
    backend::{
        AcceptedTurnCancellation, AgentBackend, BackendNotice, BackendOutcome, CompletedTurn,
        NanocodexBackend,
    },
    capabilities::{
        Capabilities, MAX_COMMAND_RECORDS, MAX_INPUT_RECORD_BYTES, MAX_OUTPUT_RECORD_BYTES,
    },
    error::{ErrorCategory, PublicError},
    protocol::{ClientFrame, TurnStart},
    strict_json,
};

const EVENT_OUTPUT_CAPACITY: usize = 240;
const NOTICE_QUEUE_CAPACITY: usize = 256;
const FINALIZING_COMMAND_DRAIN_MS: u64 = 5;
const EXACT_CANCELLATION_TIMEOUT: Duration = Duration::from_secs(1);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ServeExit {
    Success = 0,
    ProtocolOrConfig = 2,
    Authentication = 3,
    TurnFailure = 4,
    InternalOrCleanup = 5,
    Cancelled = 130,
}

impl ServeExit {
    #[must_use]
    pub const fn code(self) -> u8 {
        self as u8
    }
}

#[derive(Debug)]
struct Outbound {
    kind: String,
    request_id: Option<String>,
    command_id: Option<String>,
    session_id: Option<String>,
    data: Value,
}

struct QueuedOutbound {
    outbound: Outbound,
    _event_permit: Option<OwnedSemaphorePermit>,
}

#[derive(Clone)]
struct OutputQueue {
    sender: mpsc::UnboundedSender<QueuedOutbound>,
    event_slots: Arc<Semaphore>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum EventEnqueue {
    Sent,
    Full,
    Closed,
}

impl OutputQueue {
    // Control records are logically bounded by the physical command limit and
    // the fixed lifecycle. Staging them without awaiting stdout keeps the
    // protocol state machine responsive while the sole writer preserves order.
    async fn send_control(&self, outbound: Outbound) -> Result<(), ()> {
        self.sender
            .send(QueuedOutbound {
                outbound,
                _event_permit: None,
            })
            .map_err(|_| ())
    }

    fn try_send_event(&self, outbound: Outbound) -> EventEnqueue {
        let permit = match Arc::clone(&self.event_slots).try_acquire_owned() {
            Ok(permit) => permit,
            Err(_) => return EventEnqueue::Full,
        };
        match self.sender.send(QueuedOutbound {
            outbound,
            _event_permit: Some(permit),
        }) {
            Ok(()) => EventEnqueue::Sent,
            Err(_) => EventEnqueue::Closed,
        }
    }
}

impl Outbound {
    fn new(kind: impl Into<String>, data: Value) -> Self {
        Self {
            kind: kind.into(),
            request_id: None,
            command_id: None,
            session_id: None,
            data,
        }
    }

    fn correlated(
        mut self,
        request_id: &str,
        command_id: Option<&str>,
        session_id: Option<&str>,
    ) -> Self {
        self.request_id = Some(request_id.to_owned());
        self.command_id = command_id.map(str::to_owned);
        self.session_id = session_id.map(str::to_owned);
        self
    }
}

pub async fn serve() -> io::Result<ServeExit> {
    let external_shutdown = CancellationToken::new();
    install_signal_handler(external_shutdown.clone());
    Ok(serve_with_backend(
        tokio::io::stdin(),
        tokio::io::stdout(),
        Arc::new(NanocodexBackend),
        external_shutdown,
    )
    .await)
}

pub async fn serve_with_backend<R, W, B>(
    input: R,
    output: W,
    backend: Arc<B>,
    external_shutdown: CancellationToken,
) -> ServeExit
where
    R: AsyncRead + Unpin + Send + 'static,
    W: AsyncWrite + Unpin + Send + 'static,
    B: AgentBackend,
{
    let writer_failed = CancellationToken::new();
    let (output_sender, output_rx) = mpsc::unbounded_channel();
    let output_tx = OutputQueue {
        sender: output_sender,
        event_slots: Arc::new(Semaphore::new(EVENT_OUTPUT_CAPACITY)),
    };
    let writer_task = tokio::spawn(writer_loop(output, output_rx, writer_failed.clone()));

    if output_tx
        .send_control(Outbound::new(
            "hello",
            match serde_json::to_value(Capabilities::current()) {
                Ok(value) => value,
                Err(_) => {
                    let _ = finish_writer(output_tx, writer_task).await;
                    return ServeExit::InternalOrCleanup;
                }
            },
        ))
        .await
        .is_err()
    {
        let _ = finish_writer(output_tx, writer_task).await;
        return ServeExit::InternalOrCleanup;
    }

    let mut reader = RecordReader::new(input);
    let first_record = tokio::select! {
        record = reader.read_record() => record,
        () = external_shutdown.cancelled() => {
            let _ = send_process_failure(&output_tx, PublicError::protocol(
                "terminated_before_start",
                "The bridge was terminated before a turn started.",
            )).await;
            let _ = finish_writer(output_tx, writer_task).await;
            return ServeExit::Cancelled;
        }
        () = writer_failed.cancelled() => {
            let _ = finish_writer(output_tx, writer_task).await;
            return ServeExit::InternalOrCleanup;
        }
    };
    let record = match first_record {
        Ok(Some(record)) => record,
        Ok(None) => {
            let error = PublicError::protocol(
                "stdin_closed_before_start",
                "The command stream closed before turn.start.",
            );
            let _ = send_process_failure(&output_tx, error).await;
            let _ = finish_writer(output_tx, writer_task).await;
            return ServeExit::ProtocolOrConfig;
        }
        Err(error) => {
            let _ = send_process_failure(&output_tx, error).await;
            let _ = finish_writer(output_tx, writer_task).await;
            return ServeExit::ProtocolOrConfig;
        }
    };

    let frame = match parse_client_frame(&record) {
        Ok(frame) => frame,
        Err(error) => {
            let _ = send_process_failure(&output_tx, error).await;
            let _ = finish_writer(output_tx, writer_task).await;
            return ServeExit::ProtocolOrConfig;
        }
    };
    let mut command_ids = HashSet::new();
    command_ids.insert(frame.command_id.clone());
    let mut command_record_count = 1usize;
    let start = match frame.into_start() {
        Ok(start) => start,
        Err(error) => {
            let _ = send_process_failure(&output_tx, error).await;
            let _ = finish_writer(output_tx, writer_task).await;
            return ServeExit::ProtocolOrConfig;
        }
    };

    let turn_cancellation = CancellationToken::new();
    let (notice_tx, mut notice_rx) = mpsc::channel(NOTICE_QUEUE_CAPACITY);
    let backend_start = start.data.clone();
    let backend_cancellation = turn_cancellation.clone();
    let mut backend_task = tokio::spawn(async move {
        backend
            .run(backend_start, notice_tx, backend_cancellation)
            .await
    });

    let mut accepted_session: Option<String> = None;
    let mut accepted_cancellation: Option<Arc<dyn AcceptedTurnCancellation>> = None;
    let mut cancellation_requested = false;
    let mut cancellation_task: Option<PendingCancellation> = None;
    let mut cancellation_cause: Option<String> = None;
    let mut event_backpressure_pending = false;
    let mut input_open = true;
    let mut external_open = true;
    let mut writer_open = true;
    let mut fatal_protocol_error = None;
    let mut outcome = loop {
        tokio::select! {
            result = async {
                let pending = cancellation_task
                    .as_mut()
                    .expect("cancellation task branch requires a task");
                (&mut pending.task).await
            }, if cancellation_task.is_some() => {
                let pending = cancellation_task
                    .take()
                    .expect("completed cancellation task must still be present");
                if let Some(error) = handle_cancellation_task_result(
                    &output_tx,
                    &start,
                    accepted_session.as_deref(),
                    &mut cancellation_requested,
                    &mut cancellation_cause,
                    pending,
                    result,
                ).await {
                    if fatal_protocol_error.is_none() {
                        fatal_protocol_error = Some(error);
                    }
                    request_fallback_cancellation(
                        &turn_cancellation,
                        &mut cancellation_requested,
                        &mut cancellation_cause,
                        "cancellation_task_failed",
                    );
                }
            }
            result = &mut backend_task => {
                break match result {
                    Ok(outcome) => outcome,
                    Err(_) => BackendOutcome::Failed {
                        session_id: accepted_session.clone(),
                        error: PublicError::internal(
                            "backend_task_failed",
                            "The Nanocodex backend task stopped unexpectedly.",
                        ),
                        completed: None,
                    },
                };
            }
            notice = notice_rx.recv() => {
                if let Some(notice) = notice {
                    handle_notice(
                        &output_tx,
                        &start,
                        &mut accepted_session,
                        &mut accepted_cancellation,
                        &mut event_backpressure_pending,
                        notice,
                    ).await;
                }
            }
            record = reader.read_record(), if input_open => {
                match record {
                    Ok(Some(record)) => {
                        let command_context = RunningCommandContext {
                            accepted_session: &accepted_session,
                            accepted_cancellation: accepted_cancellation.as_ref(),
                            command_ids: &mut command_ids,
                            command_record_count: &mut command_record_count,
                            cancellation: &turn_cancellation,
                            cancellation_requested: &mut cancellation_requested,
                            cancellation_task: &mut cancellation_task,
                            cancellation_cause: &mut cancellation_cause,
                        };
                        if let Some(error) =
                            handle_running_command(&output_tx, &start, command_context, &record).await
                        {
                            input_open = false;
                            fatal_protocol_error = Some(error);
                            request_fallback_cancellation(
                                &turn_cancellation,
                                &mut cancellation_requested,
                                &mut cancellation_cause,
                                "protocol_error",
                            );
                        }
                    }
                    Ok(None) => {
                        input_open = false;
                        request_fallback_cancellation(
                            &turn_cancellation,
                            &mut cancellation_requested,
                            &mut cancellation_cause,
                            "stdin_eof",
                        );
                    }
                    Err(error) => {
                        input_open = false;
                        fatal_protocol_error = Some(error);
                        request_fallback_cancellation(
                            &turn_cancellation,
                            &mut cancellation_requested,
                            &mut cancellation_cause,
                            "stdin_error",
                        );
                    }
                }
            }
            () = external_shutdown.cancelled(), if external_open => {
                external_open = false;
                request_fallback_cancellation(
                    &turn_cancellation,
                    &mut cancellation_requested,
                    &mut cancellation_cause,
                    "external_signal",
                );
            }
            () = writer_failed.cancelled(), if writer_open => {
                writer_open = false;
                request_fallback_cancellation(
                    &turn_cancellation,
                    &mut cancellation_requested,
                    &mut cancellation_cause,
                    "output_closed",
                );
            }
        }
    };

    // Drain already-queued notices before the terminal so ordering remains
    // deterministic even when backend completion and event delivery race.
    while let Ok(notice) = notice_rx.try_recv() {
        handle_notice(
            &output_tx,
            &start,
            &mut accepted_session,
            &mut accepted_cancellation,
            &mut event_backpressure_pending,
            notice,
        )
        .await;
    }

    if let Some(mut pending) = cancellation_task.take() {
        if pending.task.is_finished() {
            let result = (&mut pending.task).await;
            if let Some(error) = handle_cancellation_task_result(
                &output_tx,
                &start,
                accepted_session.as_deref(),
                &mut cancellation_requested,
                &mut cancellation_cause,
                pending,
                result,
            )
            .await
                && fatal_protocol_error.is_none()
            {
                fatal_protocol_error = Some(error);
            }
        } else {
            pending.task.abort();
            let _ = send_command_rejected(
                &output_tx,
                &start.request_id,
                Some(&pending.command_id),
                accepted_session.as_deref(),
                PublicError::protocol(
                    "turn_not_cancellable",
                    "The turn has already entered finalization.",
                ),
            )
            .await;
        }
    }

    if input_open
        && let Some(error) = drain_finalizing_commands(
            &mut reader,
            &output_tx,
            &start,
            accepted_session.as_deref(),
            &mut command_ids,
            &mut command_record_count,
        )
        .await
    {
        fatal_protocol_error = Some(error);
    }

    flush_final_event_backpressure_marker(
        &output_tx,
        &start.request_id,
        accepted_session.as_deref(),
        &mut event_backpressure_pending,
    )
    .await;

    if let Some(error) = fatal_protocol_error {
        outcome = BackendOutcome::Failed {
            session_id: accepted_session.clone(),
            error,
            completed: None,
        };
    }

    let exit = emit_terminal(
        &output_tx,
        &start,
        accepted_session.as_deref(),
        outcome,
        cancellation_cause.as_deref(),
    )
    .await;
    if finish_writer(output_tx, writer_task).await {
        exit
    } else {
        ServeExit::InternalOrCleanup
    }
}

async fn handle_notice(
    output: &OutputQueue,
    start: &TurnStart,
    accepted_session: &mut Option<String>,
    accepted_cancellation: &mut Option<Arc<dyn AcceptedTurnCancellation>>,
    event_backpressure_pending: &mut bool,
    notice: BackendNotice,
) {
    match notice {
        BackendNotice::Accepted {
            session_id,
            cancellation,
        } => {
            if accepted_session.is_some() {
                return;
            }
            *accepted_session = Some(session_id.clone());
            *accepted_cancellation = Some(cancellation);
            let _ = output
                .send_control(Outbound::new("turn.accepted", json!({})).correlated(
                    &start.request_id,
                    Some(&start.command_id),
                    Some(&session_id),
                ))
                .await;
        }
        BackendNotice::Event { event } => {
            let Some(session_id) = accepted_session.as_deref() else {
                return;
            };
            flush_event_backpressure_marker(
                output,
                &start.request_id,
                Some(session_id),
                event_backpressure_pending,
            );
            enqueue_event(
                output,
                Outbound::new("agent.event", json!({"event": event})).correlated(
                    &start.request_id,
                    None,
                    Some(session_id),
                ),
                event_backpressure_pending,
            );
        }
        BackendNotice::EventTruncated {
            upstream_type,
            upstream_seq,
            reason,
        } => {
            let Some(session_id) = accepted_session.as_deref() else {
                return;
            };
            flush_event_backpressure_marker(
                output,
                &start.request_id,
                Some(session_id),
                event_backpressure_pending,
            );
            enqueue_event(
                output,
                Outbound::new(
                    "agent.event-truncated",
                    json!({
                        "upstreamType": upstream_type,
                        "upstreamSeq": upstream_seq,
                        "reason": reason,
                    }),
                )
                .correlated(&start.request_id, None, Some(session_id)),
                event_backpressure_pending,
            );
        }
    }
}

fn enqueue_event(output: &OutputQueue, outbound: Outbound, pending: &mut bool) {
    if *pending {
        return;
    }
    match output.try_send_event(outbound) {
        EventEnqueue::Sent => {}
        EventEnqueue::Full => *pending = true,
        EventEnqueue::Closed => {}
    }
}

fn flush_event_backpressure_marker(
    output: &OutputQueue,
    request_id: &str,
    session_id: Option<&str>,
    pending: &mut bool,
) {
    if !*pending {
        return;
    }
    let Some(session_id) = session_id else {
        return;
    };
    let marker = event_backpressure_marker(request_id, session_id);
    if output.try_send_event(marker) == EventEnqueue::Sent {
        *pending = false;
    }
}

async fn flush_final_event_backpressure_marker(
    output: &OutputQueue,
    request_id: &str,
    session_id: Option<&str>,
    pending: &mut bool,
) {
    if !*pending {
        return;
    }
    let Some(session_id) = session_id else {
        return;
    };
    if output
        .send_control(event_backpressure_marker(request_id, session_id))
        .await
        .is_ok()
    {
        *pending = false;
    }
}

fn event_backpressure_marker(request_id: &str, session_id: &str) -> Outbound {
    Outbound::new(
        "agent.event-truncated",
        json!({
            "upstreamType": null,
            "upstreamSeq": null,
            "reason": "bridge_event_backpressure",
        }),
    )
    .correlated(request_id, None, Some(session_id))
}

struct RunningCommandContext<'a> {
    accepted_session: &'a Option<String>,
    accepted_cancellation: Option<&'a Arc<dyn AcceptedTurnCancellation>>,
    command_ids: &'a mut HashSet<String>,
    command_record_count: &'a mut usize,
    cancellation: &'a CancellationToken,
    cancellation_requested: &'a mut bool,
    cancellation_task: &'a mut Option<PendingCancellation>,
    cancellation_cause: &'a mut Option<String>,
}

async fn handle_running_command(
    output: &OutputQueue,
    start: &TurnStart,
    state: RunningCommandContext<'_>,
    record: &[u8],
) -> Option<PublicError> {
    if *state.command_record_count >= MAX_COMMAND_RECORDS {
        return Some(too_many_commands_error());
    }
    *state.command_record_count += 1;
    let frame = match parse_client_frame(record) {
        Ok(frame) => frame,
        Err(error) => return Some(error),
    };
    let command_id = frame.command_id.clone();
    if state.command_ids.contains(&command_id) {
        let _ = send_command_rejected(
            output,
            &start.request_id,
            Some(&command_id),
            state.accepted_session.as_deref(),
            PublicError::protocol("duplicate_command_id", "commandId was already used."),
        )
        .await;
        return None;
    }
    state.command_ids.insert(command_id.clone());
    let cancel = match frame.into_cancel() {
        Ok(cancel) => cancel,
        Err(error) => {
            let _ = send_command_rejected(
                output,
                &start.request_id,
                Some(&command_id),
                state.accepted_session.as_deref(),
                error,
            )
            .await;
            return None;
        }
    };
    let session_matches = match (
        state.accepted_session.as_deref(),
        cancel.session_id.as_deref(),
    ) {
        (Some(active), Some(target)) => active == target,
        (None, None) => true,
        _ => false,
    };
    if cancel.request_id != start.request_id || !session_matches {
        let _ = send_command_rejected(
            output,
            &start.request_id,
            Some(&cancel.command_id),
            state.accepted_session.as_deref(),
            PublicError::protocol(
                "correlation_mismatch",
                "The cancellation target does not match the active turn.",
            ),
        )
        .await;
        return None;
    }
    if *state.cancellation_requested || state.cancellation_task.is_some() {
        let _ = send_command_rejected(
            output,
            &start.request_id,
            Some(&cancel.command_id),
            state.accepted_session.as_deref(),
            PublicError::protocol(
                "cancellation_in_progress",
                "Cancellation is already in progress.",
            ),
        )
        .await;
        return None;
    }
    let reason = cancel.data.reason.unwrap_or_else(|| "cancelled".to_owned());
    if state.accepted_session.is_none() {
        *state.cancellation_requested = true;
        set_cancellation_cause(state.cancellation_cause, &reason);
        state.cancellation.cancel();
        let _ =
            output
                .send_control(
                    Outbound::new("command.accepted", json!({"command": "turn.cancel"}))
                        .correlated(&start.request_id, Some(&cancel.command_id), None),
                )
                .await;
        return None;
    }

    let Some(exact_cancellation) = state.accepted_cancellation.cloned() else {
        return Some(PublicError::internal(
            "missing_turn_cancellation",
            "The accepted turn has no exact cancellation handle.",
        ));
    };
    *state.cancellation_requested = true;
    let task = tokio::spawn(async move {
        match tokio::time::timeout(EXACT_CANCELLATION_TIMEOUT, exact_cancellation.cancel()).await {
            Ok(result) => result,
            Err(_) => Err(PublicError::internal(
                "cancellation_timed_out",
                "Exact turn cancellation did not settle within the bridge grace period.",
            )),
        }
    });
    *state.cancellation_task = Some(PendingCancellation {
        command_id,
        reason,
        task,
    });
    None
}

async fn drain_finalizing_commands<R>(
    reader: &mut RecordReader<R>,
    output: &OutputQueue,
    start: &TurnStart,
    accepted_session: Option<&str>,
    command_ids: &mut HashSet<String>,
    command_record_count: &mut usize,
) -> Option<PublicError>
where
    R: AsyncRead + Unpin,
{
    let deadline =
        tokio::time::Instant::now() + std::time::Duration::from_millis(FINALIZING_COMMAND_DRAIN_MS);
    // One extra read is necessary to detect a physical record beyond the
    // advertised limit. Blank records are filtered by RecordReader and do not
    // consume this budget.
    for _ in *command_record_count..=MAX_COMMAND_RECORDS {
        let record = match tokio::time::timeout_at(deadline, reader.read_record()).await {
            Ok(Ok(Some(record))) => record,
            Ok(Ok(None)) | Err(_) => return None,
            Ok(Err(error)) => return Some(error),
        };
        if let Some(error) = reject_finalizing_command(
            output,
            start,
            accepted_session,
            command_ids,
            command_record_count,
            &record,
        )
        .await
        {
            return Some(error);
        }
    }
    Some(too_many_commands_error())
}

async fn reject_finalizing_command(
    output: &OutputQueue,
    start: &TurnStart,
    accepted_session: Option<&str>,
    command_ids: &mut HashSet<String>,
    command_record_count: &mut usize,
    record: &[u8],
) -> Option<PublicError> {
    if *command_record_count >= MAX_COMMAND_RECORDS {
        return Some(too_many_commands_error());
    }
    *command_record_count += 1;
    let frame = match parse_client_frame(record) {
        Ok(frame) => frame,
        Err(error) => return Some(error),
    };
    let command_id = frame.command_id.clone();
    if command_ids.contains(&command_id) {
        let _ = send_command_rejected(
            output,
            &start.request_id,
            Some(&command_id),
            accepted_session,
            PublicError::protocol("duplicate_command_id", "commandId was already used."),
        )
        .await;
        return None;
    }
    command_ids.insert(command_id.clone());
    let cancel = match frame.into_cancel() {
        Ok(cancel) => cancel,
        Err(error) => {
            let _ = send_command_rejected(
                output,
                &start.request_id,
                Some(&command_id),
                accepted_session,
                error,
            )
            .await;
            return None;
        }
    };
    let session_matches = match (accepted_session, cancel.session_id.as_deref()) {
        (Some(active), Some(target)) => active == target,
        (None, None) => true,
        _ => false,
    };
    let error = if cancel.request_id != start.request_id || !session_matches {
        PublicError::protocol(
            "correlation_mismatch",
            "The cancellation target does not match the active turn.",
        )
    } else {
        PublicError::protocol(
            "turn_not_cancellable",
            "The turn has already entered finalization.",
        )
    };
    let _ = send_command_rejected(
        output,
        &start.request_id,
        Some(&cancel.command_id),
        accepted_session,
        error,
    )
    .await;
    None
}

fn too_many_commands_error() -> PublicError {
    PublicError::protocol(
        "too_many_commands",
        "The command stream exceeds the advertised per-process limit.",
    )
}

struct PendingCancellation {
    command_id: String,
    reason: String,
    task: JoinHandle<Result<(), PublicError>>,
}

struct CancellationReply {
    command_id: String,
    reason: String,
    result: Result<(), PublicError>,
}

async fn handle_cancellation_task_result(
    output: &OutputQueue,
    start: &TurnStart,
    session_id: Option<&str>,
    cancellation_requested: &mut bool,
    cancellation_cause: &mut Option<String>,
    pending: PendingCancellation,
    result: Result<Result<(), PublicError>, tokio::task::JoinError>,
) -> Option<PublicError> {
    let result = result.unwrap_or_else(|_| {
        Err(PublicError::internal(
            "cancellation_task_failed",
            "The exact turn cancellation task stopped unexpectedly.",
        ))
    });
    let fatal_error = result
        .as_ref()
        .err()
        .filter(|error| error.category == ErrorCategory::Internal)
        .cloned();
    handle_cancellation_reply(
        output,
        start,
        session_id,
        cancellation_requested,
        cancellation_cause,
        CancellationReply {
            command_id: pending.command_id,
            reason: pending.reason,
            result,
        },
    )
    .await;
    fatal_error
}

async fn handle_cancellation_reply(
    output: &OutputQueue,
    start: &TurnStart,
    session_id: Option<&str>,
    cancellation_requested: &mut bool,
    cancellation_cause: &mut Option<String>,
    reply: CancellationReply,
) {
    match reply.result {
        Ok(()) => {
            *cancellation_requested = true;
            set_cancellation_cause(cancellation_cause, &reply.reason);
            let _ = output
                .send_control(
                    Outbound::new("command.accepted", json!({"command": "turn.cancel"}))
                        .correlated(&start.request_id, Some(&reply.command_id), session_id),
                )
                .await;
        }
        Err(error) => {
            let _ = send_command_rejected(
                output,
                &start.request_id,
                Some(&reply.command_id),
                session_id,
                error,
            )
            .await;
        }
    }
}

fn set_cancellation_cause(cause: &mut Option<String>, value: &str) {
    if cause.is_none() {
        *cause = Some(value.to_owned());
    }
}

fn request_fallback_cancellation(
    cancellation: &CancellationToken,
    cancellation_requested: &mut bool,
    cancellation_cause: &mut Option<String>,
    cause: &str,
) {
    *cancellation_requested = true;
    set_cancellation_cause(cancellation_cause, cause);
    cancellation.cancel();
}

async fn emit_terminal(
    output: &OutputQueue,
    start: &TurnStart,
    accepted_session: Option<&str>,
    outcome: BackendOutcome,
    cancellation_cause: Option<&str>,
) -> ServeExit {
    // Once an initiating cancellation has been accepted, a secondary cleanup
    // failure cannot overwrite its public classification. A completed snapshot
    // is the spec §6 recovery exception and must still be published.
    if let (
        Some(reason),
        BackendOutcome::Failed {
            error,
            session_id: _,
            completed,
        },
    ) = (cancellation_cause, &outcome)
        && error.category == ErrorCategory::Cleanup
        && completed.is_none()
    {
        return emit_cancelled_terminal(output, start, accepted_session, reason).await;
    }

    match outcome {
        BackendOutcome::Completed(completed) => {
            let Some(session_id) = accepted_session else {
                let _ = send_process_failure(
                    output,
                    PublicError::internal(
                        "completion_before_acceptance",
                        "The backend completed before the turn was accepted.",
                    ),
                )
                .await;
                return ServeExit::InternalOrCleanup;
            };
            if completed.session_id != session_id {
                let error = PublicError::internal(
                    "session_identity_mismatch",
                    "The completed session does not match the accepted turn.",
                );
                let _ = output
                    .send_control(
                        Outbound::new("turn.failed", json!({"error": error})).correlated(
                            &start.request_id,
                            None,
                            Some(session_id),
                        ),
                    )
                    .await;
                return ServeExit::InternalOrCleanup;
            }
            let terminal = Outbound::new("turn.completed", completed_json(completed)).correlated(
                &start.request_id,
                None,
                Some(session_id),
            );
            match send_terminal_record(output, terminal).await {
                TerminalSend::Sent => ServeExit::Success,
                TerminalSend::Replaced | TerminalSend::Closed => ServeExit::InternalOrCleanup,
            }
        }
        BackendOutcome::Cancelled { session_id: _ } => {
            emit_cancelled_terminal(
                output,
                start,
                accepted_session,
                cancellation_cause.unwrap_or("backend_cancelled"),
            )
            .await
        }
        BackendOutcome::Failed {
            session_id: _,
            error,
            completed,
        } => {
            let exit = exit_for_error(&error);
            let mut data = json!({"error": error});
            if let Some(completed) = completed {
                data["completed"] = recovery_json(completed);
            }
            let kind = if accepted_session.is_some() {
                "turn.failed"
            } else {
                "process.failed"
            };
            let session_id = accepted_session;
            let terminal =
                Outbound::new(kind, data).correlated(&start.request_id, None, session_id);
            match send_terminal_record(output, terminal).await {
                TerminalSend::Sent => exit,
                TerminalSend::Replaced | TerminalSend::Closed => ServeExit::InternalOrCleanup,
            }
        }
    }
}

async fn emit_cancelled_terminal(
    output: &OutputQueue,
    start: &TurnStart,
    accepted_session: Option<&str>,
    reason: &str,
) -> ServeExit {
    let kind = if accepted_session.is_some() {
        "turn.cancelled"
    } else {
        "process.failed"
    };
    let data = if accepted_session.is_some() {
        json!({"reason": reason})
    } else {
        json!({"error": PublicError::protocol(
            "cancelled_before_acceptance",
            "The turn was cancelled before it was accepted.",
        )})
    };
    // Session identity is public only after turn.accepted. Nanocodex may have
    // constructed an internal session before prompt acceptance, but a
    // process.failed record cannot correlate to it.
    let terminal = Outbound::new(kind, data).correlated(&start.request_id, None, accepted_session);
    match send_terminal_record(output, terminal).await {
        TerminalSend::Sent => ServeExit::Cancelled,
        TerminalSend::Replaced | TerminalSend::Closed => ServeExit::InternalOrCleanup,
    }
}

fn completed_json(completed: CompletedTurn) -> Value {
    json!({
        "finalMessage": completed.final_message,
        "usage": completed.usage,
        "model": completed_model(&completed.snapshot),
        "snapshotVersion": completed.snapshot_version,
        "snapshot": completed.snapshot,
        "canonicalWorkspace": completed.canonical_workspace,
    })
}

fn recovery_json(completed: CompletedTurn) -> Value {
    // A cleanup failure rejects the generation, so only the durable recovery
    // boundary is needed. Omitting the duplicate final text and usage keeps a
    // maximum-sized valid snapshot representable in the terminal frame.
    json!({
        "model": completed_model(&completed.snapshot),
        "snapshotVersion": completed.snapshot_version,
        "snapshot": completed.snapshot,
        "canonicalWorkspace": completed.canonical_workspace,
    })
}

fn completed_model(snapshot: &Value) -> &'static str {
    snapshot
        .get("model")
        .and_then(Value::as_str)
        .and_then(crate::protocol::ModelId::parse)
        .unwrap_or(crate::protocol::ModelId::Sol)
        .as_str()
}

fn parse_client_frame(record: &[u8]) -> Result<ClientFrame, PublicError> {
    let frame: ClientFrame = strict_json::from_slice(record).map_err(|_| {
        PublicError::protocol(
            "invalid_json",
            "The command record is not valid strict JSON.",
        )
    })?;
    frame.validate_envelope()?;
    Ok(frame)
}

async fn send_process_failure(output: &OutputQueue, error: PublicError) -> Result<(), ()> {
    output
        .send_control(Outbound::new("process.failed", json!({"error": error})))
        .await
}

async fn send_command_rejected(
    output: &OutputQueue,
    request_id: &str,
    command_id: Option<&str>,
    session_id: Option<&str>,
    error: PublicError,
) -> Result<(), ()> {
    output
        .send_control(
            Outbound::new("command.rejected", json!({"error": error}))
                .correlated(request_id, command_id, session_id),
        )
        .await
}

fn exit_for_error(error: &PublicError) -> ServeExit {
    match error.category {
        ErrorCategory::Protocol
        | ErrorCategory::Config
        | ErrorCategory::Checkpoint
        | ErrorCategory::Workspace => ServeExit::ProtocolOrConfig,
        ErrorCategory::Auth => ServeExit::Authentication,
        ErrorCategory::Provider | ErrorCategory::Tool => ServeExit::TurnFailure,
        ErrorCategory::Cleanup | ErrorCategory::Internal => ServeExit::InternalOrCleanup,
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum TerminalSend {
    Sent,
    Replaced,
    Closed,
}

async fn send_terminal_record(output: &OutputQueue, terminal: Outbound) -> TerminalSend {
    if outbound_fits_limit(&terminal, u64::MAX, MAX_OUTPUT_RECORD_BYTES) {
        return if output.send_control(terminal).await.is_ok() {
            TerminalSend::Sent
        } else {
            TerminalSend::Closed
        };
    }

    let fallback_kind = if terminal.session_id.is_some() {
        "turn.failed"
    } else {
        "process.failed"
    };
    let mut fallback = Outbound::new(
        fallback_kind,
        json!({"error": PublicError::internal(
            "result_too_large",
            "The turn result exceeds the advertised output limit.",
        )}),
    );
    fallback.request_id = terminal.request_id;
    fallback.session_id = terminal.session_id;
    if output.send_control(fallback).await.is_ok() {
        TerminalSend::Replaced
    } else {
        TerminalSend::Closed
    }
}

struct BoundedCounter {
    bytes: usize,
    limit: usize,
}

impl io::Write for BoundedCounter {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        if self.bytes.saturating_add(buffer.len()) > self.limit {
            return Err(io::Error::other("serialized record exceeds limit"));
        }
        self.bytes += buffer.len();
        Ok(buffer.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

fn outbound_fits_limit(outbound: &Outbound, seq: u64, limit: usize) -> bool {
    let mut counter = BoundedCounter { bytes: 0, limit };
    serde_json::to_writer(
        &mut counter,
        &ServerFrameRef {
            protocol: crate::capabilities::BRIDGE_PROTOCOL_NAME,
            version: crate::capabilities::BRIDGE_PROTOCOL_VERSION,
            kind: &outbound.kind,
            seq,
            request_id: outbound.request_id.as_deref(),
            command_id: outbound.command_id.as_deref(),
            session_id: outbound.session_id.as_deref(),
            data: &outbound.data,
        },
    )
    .is_ok()
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ServerFrameRef<'a> {
    protocol: &'static str,
    version: u16,
    #[serde(rename = "type")]
    kind: &'a str,
    seq: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    request_id: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    command_id: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    session_id: Option<&'a str>,
    data: &'a Value,
}

fn encoded_outbound(outbound: &Outbound, seq: u64) -> Result<Vec<u8>, serde_json::Error> {
    serde_json::to_vec(&ServerFrameRef {
        protocol: crate::capabilities::BRIDGE_PROTOCOL_NAME,
        version: crate::capabilities::BRIDGE_PROTOCOL_VERSION,
        kind: &outbound.kind,
        seq,
        request_id: outbound.request_id.as_deref(),
        command_id: outbound.command_id.as_deref(),
        session_id: outbound.session_id.as_deref(),
        data: &outbound.data,
    })
}

async fn writer_loop<W>(
    mut output: W,
    mut records: mpsc::UnboundedReceiver<QueuedOutbound>,
    failed: CancellationToken,
) -> io::Result<()>
where
    W: AsyncWrite + Unpin,
{
    let mut seq = 1u64;
    while let Some(queued) = records.recv().await {
        let outbound = queued.outbound;
        let mut encoded = encoded_outbound(&outbound, seq).map_err(io::Error::other)?;
        seq = seq
            .checked_add(1)
            .ok_or_else(|| io::Error::other("protocol sequence overflow"))?;
        if encoded.len() > MAX_OUTPUT_RECORD_BYTES {
            failed.cancel();
            return Err(io::Error::other("protocol output record exceeds limit"));
        }
        encoded.push(b'\n');
        if let Err(error) = output.write_all(&encoded).await {
            failed.cancel();
            return Err(error);
        }
        if let Err(error) = output.flush().await {
            failed.cancel();
            return Err(error);
        }
    }
    Ok(())
}

struct RecordReader<R> {
    reader: BufReader<R>,
    record: Vec<u8>,
}

impl<R> RecordReader<R>
where
    R: AsyncRead + Unpin,
{
    fn new(input: R) -> Self {
        Self {
            reader: BufReader::new(input),
            record: Vec::new(),
        }
    }

    async fn read_record(&mut self) -> Result<Option<Vec<u8>>, PublicError> {
        loop {
            let available = self.reader.fill_buf().await.map_err(|_| {
                PublicError::protocol("stdin_read_failed", "The command stream could not be read.")
            })?;
            if available.is_empty() {
                return if self.record.is_empty() {
                    Ok(None)
                } else {
                    Ok(Some(std::mem::take(&mut self.record)))
                };
            }
            let newline = available.iter().position(|byte| *byte == b'\n');
            let take = newline.map_or(available.len(), |index| index);
            if self.record.len().saturating_add(take) > MAX_INPUT_RECORD_BYTES {
                return Err(PublicError::protocol(
                    "input_record_too_large",
                    "The command record exceeds the advertised input limit.",
                ));
            }
            self.record.extend_from_slice(&available[..take]);
            let consumed = take + usize::from(newline.is_some());
            self.reader.consume(consumed);
            if newline.is_some() {
                if self.record.last() == Some(&b'\r') {
                    self.record.pop();
                }
                if self.record.is_empty() {
                    continue;
                }
                return Ok(Some(std::mem::take(&mut self.record)));
            }
        }
    }
}

async fn finish_writer(output: OutputQueue, task: tokio::task::JoinHandle<io::Result<()>>) -> bool {
    drop(output);
    matches!(task.await, Ok(Ok(())))
}

fn install_signal_handler(token: CancellationToken) {
    tokio::spawn(async move {
        #[cfg(unix)]
        {
            use tokio::signal::unix::{SignalKind, signal};
            let mut terminate = signal(SignalKind::terminate()).ok();
            tokio::select! {
                _ = tokio::signal::ctrl_c() => {}
                _ = async {
                    if let Some(signal) = terminate.as_mut() {
                        signal.recv().await;
                    } else {
                        std::future::pending::<()>().await;
                    }
                } => {}
            }
        }
        #[cfg(not(unix))]
        {
            let _ = tokio::signal::ctrl_c().await;
        }
        token.cancel();
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use futures_util::task::AtomicWaker;
    use std::{
        future::pending,
        pin::Pin,
        sync::{
            Mutex,
            atomic::{AtomicBool, AtomicUsize, Ordering},
        },
        task::{Context, Poll},
    };
    use tokio::io::{AsyncBufRead, AsyncReadExt, AsyncWrite};

    use crate::{
        backend::{AgentBackend, BackendNotice, BackendOutcome, CompletedTurn},
        capabilities::MAX_SNAPSHOT_BYTES,
        protocol::TurnStartData,
    };

    #[derive(Clone, Copy)]
    enum FakeBehavior {
        Complete,
        FailBeforeAcceptanceWithSession,
        SecondaryCleanupAfterCancellation,
        CleanupFailedAfterCompleted,
        WaitForCancellation,
        WaitBeforeAcceptance,
    }

    struct FakeBackend {
        behavior: FakeBehavior,
    }

    struct FakeTurnCancellation {
        cancellation: CancellationToken,
    }

    struct ObservedCancellation {
        cancellation: CancellationToken,
        observed: Arc<AtomicBool>,
    }

    struct HangingCancellation {
        started: Arc<tokio::sync::Notify>,
    }

    #[async_trait]
    impl AcceptedTurnCancellation for HangingCancellation {
        async fn cancel(&self) -> Result<(), PublicError> {
            self.started.notify_one();
            pending().await
        }
    }

    struct PanickingCancellation;

    #[async_trait]
    impl AcceptedTurnCancellation for PanickingCancellation {
        async fn cancel(&self) -> Result<(), PublicError> {
            panic!("synthetic exact cancellation panic")
        }
    }

    #[async_trait]
    impl AcceptedTurnCancellation for ObservedCancellation {
        async fn cancel(&self) -> Result<(), PublicError> {
            self.observed.store(true, Ordering::SeqCst);
            self.cancellation.cancel();
            Ok(())
        }
    }

    struct FloodBackend {
        cancellation_observed: Arc<AtomicBool>,
    }

    struct FragmentBackend {
        emit_event: Arc<tokio::sync::Notify>,
    }

    struct ControlledCancellationBackend {
        exact_cancellation: Arc<dyn AcceptedTurnCancellation>,
        completion_release: Option<Arc<tokio::sync::Notify>>,
        fallback_observed: Arc<AtomicBool>,
    }

    #[async_trait]
    impl AgentBackend for ControlledCancellationBackend {
        async fn run(
            &self,
            request: TurnStartData,
            notices: mpsc::Sender<BackendNotice>,
            cancellation: CancellationToken,
        ) -> BackendOutcome {
            let session_id = "controlled-session".to_owned();
            notices
                .send(BackendNotice::Accepted {
                    session_id: session_id.clone(),
                    cancellation: Arc::clone(&self.exact_cancellation),
                })
                .await
                .unwrap();

            if let Some(release) = &self.completion_release {
                tokio::select! {
                    () = release.notified() => {
                        return BackendOutcome::Completed(CompletedTurn {
                            session_id,
                            final_message: "complete".to_owned(),
                            usage: json!({"inputTokens": 1, "outputTokens": 1}),
                            snapshot_version: 1,
                            snapshot: json!({"version": 1}),
                            canonical_workspace: request.workspace.display().to_string(),
                        });
                    }
                    () = cancellation.cancelled() => {}
                }
            } else {
                cancellation.cancelled().await;
            }
            self.fallback_observed.store(true, Ordering::SeqCst);
            BackendOutcome::Cancelled {
                session_id: Some(session_id),
            }
        }
    }

    struct GatedWriter {
        state: Arc<GatedWriterState>,
    }

    struct GatedWriterState {
        records_written: AtomicUsize,
        blocked: AtomicBool,
        waker: AtomicWaker,
        bytes: Mutex<Vec<u8>>,
    }

    impl GatedWriterState {
        fn release(&self) {
            self.blocked.store(false, Ordering::SeqCst);
            self.waker.wake();
        }
    }

    impl AsyncWrite for GatedWriter {
        fn poll_write(
            self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            buffer: &[u8],
        ) -> Poll<io::Result<usize>> {
            if self.state.blocked.load(Ordering::SeqCst) {
                self.state.waker.register(cx.waker());
                if self.state.blocked.load(Ordering::SeqCst) {
                    return Poll::Pending;
                }
            }
            self.state.bytes.lock().unwrap().extend_from_slice(buffer);
            if self.state.records_written.fetch_add(1, Ordering::SeqCst) + 1 == 2 {
                self.state.blocked.store(true, Ordering::SeqCst);
            }
            Poll::Ready(Ok(buffer.len()))
        }

        fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }

        fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }

    struct RejectingCancellation {
        calls: Arc<AtomicUsize>,
    }

    struct NotifyingRejectCancellation {
        invoked: Arc<tokio::sync::Notify>,
    }

    #[async_trait]
    impl AcceptedTurnCancellation for NotifyingRejectCancellation {
        async fn cancel(&self) -> Result<(), PublicError> {
            self.invoked.notify_one();
            Err(PublicError::protocol(
                "turn_not_cancellable",
                "The turn has already entered finalization.",
            ))
        }
    }

    struct LateCompletionBackend {
        cancellation_invoked: Arc<tokio::sync::Notify>,
    }

    #[async_trait]
    impl AgentBackend for LateCompletionBackend {
        async fn run(
            &self,
            request: TurnStartData,
            notices: mpsc::Sender<BackendNotice>,
            _cancellation: CancellationToken,
        ) -> BackendOutcome {
            let session_id = "fake-session".to_owned();
            notices
                .send(BackendNotice::Accepted {
                    session_id: session_id.clone(),
                    cancellation: Arc::new(NotifyingRejectCancellation {
                        invoked: Arc::clone(&self.cancellation_invoked),
                    }),
                })
                .await
                .unwrap();
            self.cancellation_invoked.notified().await;
            BackendOutcome::Completed(CompletedTurn {
                session_id,
                final_message: "already complete".to_owned(),
                usage: json!({"inputTokens": 2, "outputTokens": 2}),
                snapshot_version: 1,
                snapshot: json!({"version": 1}),
                canonical_workspace: request.workspace.display().to_string(),
            })
        }
    }

    #[async_trait]
    impl AcceptedTurnCancellation for RejectingCancellation {
        async fn cancel(&self) -> Result<(), PublicError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Err(PublicError::protocol(
                "turn_not_cancellable",
                "The turn has already entered finalization.",
            ))
        }
    }

    struct CompletionRaceBackend {
        release: Arc<tokio::sync::Notify>,
        cancellation_calls: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl AgentBackend for CompletionRaceBackend {
        async fn run(
            &self,
            request: TurnStartData,
            notices: mpsc::Sender<BackendNotice>,
            cancellation: CancellationToken,
        ) -> BackendOutcome {
            let session_id = "finalizing-session".to_owned();
            notices
                .send(BackendNotice::Accepted {
                    session_id: session_id.clone(),
                    cancellation: Arc::new(RejectingCancellation {
                        calls: Arc::clone(&self.cancellation_calls),
                    }),
                })
                .await
                .unwrap();
            tokio::select! {
                () = self.release.notified() => {}
                () = cancellation.cancelled() => {
                    self.cancellation_calls.fetch_add(100, Ordering::SeqCst);
                    return BackendOutcome::Cancelled { session_id: Some(session_id) };
                }
            }
            BackendOutcome::Completed(CompletedTurn {
                session_id,
                final_message: "complete".to_owned(),
                usage: json!({"inputTokens": 1, "outputTokens": 1}),
                snapshot_version: 1,
                snapshot: json!({"version": 1}),
                canonical_workspace: request.workspace.display().to_string(),
            })
        }
    }

    #[async_trait]
    impl AgentBackend for FloodBackend {
        async fn run(
            &self,
            _request: TurnStartData,
            notices: mpsc::Sender<BackendNotice>,
            cancellation: CancellationToken,
        ) -> BackendOutcome {
            let session_id = "flood-session".to_owned();
            notices
                .send(BackendNotice::Accepted {
                    session_id: session_id.clone(),
                    cancellation: Arc::new(ObservedCancellation {
                        cancellation: cancellation.clone(),
                        observed: Arc::clone(&self.cancellation_observed),
                    }),
                })
                .await
                .unwrap();
            for seq in 0..(NOTICE_QUEUE_CAPACITY * 8) {
                let _ = notices.try_send(BackendNotice::Event {
                    event: json!({
                        "type": "assistant.delta",
                        "upstreamSeq": seq,
                        "payload": {"text": "event-pressure"},
                    }),
                });
            }
            cancellation.cancelled().await;
            BackendOutcome::Cancelled {
                session_id: Some(session_id),
            }
        }
    }

    #[async_trait]
    impl AgentBackend for FragmentBackend {
        async fn run(
            &self,
            _request: TurnStartData,
            notices: mpsc::Sender<BackendNotice>,
            cancellation: CancellationToken,
        ) -> BackendOutcome {
            let session_id = "fragment-session".to_owned();
            notices
                .send(BackendNotice::Accepted {
                    session_id: session_id.clone(),
                    cancellation: Arc::new(FakeTurnCancellation {
                        cancellation: cancellation.clone(),
                    }),
                })
                .await
                .unwrap();
            self.emit_event.notified().await;
            notices
                .send(BackendNotice::Event {
                    event: json!({
                        "type": "assistant.delta",
                        "upstreamSeq": 1,
                        "payload": {"text": "interleaved-event"},
                    }),
                })
                .await
                .unwrap();
            cancellation.cancelled().await;
            BackendOutcome::Cancelled {
                session_id: Some(session_id),
            }
        }
    }

    #[async_trait]
    impl AcceptedTurnCancellation for FakeTurnCancellation {
        async fn cancel(&self) -> Result<(), PublicError> {
            self.cancellation.cancel();
            Ok(())
        }
    }

    #[async_trait]
    impl AgentBackend for FakeBackend {
        async fn run(
            &self,
            request: TurnStartData,
            notices: mpsc::Sender<BackendNotice>,
            cancellation: CancellationToken,
        ) -> BackendOutcome {
            let session_id = "fake-session".to_owned();
            if matches!(self.behavior, FakeBehavior::FailBeforeAcceptanceWithSession) {
                return BackendOutcome::Failed {
                    session_id: Some(session_id),
                    error: PublicError::new(
                        "prompt_rejected",
                        ErrorCategory::Provider,
                        "The prompt was rejected before turn acceptance.",
                        crate::error::RetryDisposition::Never,
                    ),
                    completed: None,
                };
            }
            if matches!(self.behavior, FakeBehavior::WaitBeforeAcceptance) {
                cancellation.cancelled().await;
                return BackendOutcome::Cancelled { session_id: None };
            }
            notices
                .send(BackendNotice::Accepted {
                    session_id: session_id.clone(),
                    cancellation: Arc::new(FakeTurnCancellation {
                        cancellation: cancellation.clone(),
                    }),
                })
                .await
                .unwrap();
            match self.behavior {
                FakeBehavior::Complete => {
                    notices
                        .send(BackendNotice::Event {
                            event: json!({
                                "protocol_version": 1,
                                "request_id": "upstream-request",
                                "seq": 0,
                                "type": "assistant.delta",
                                "payload": {"text": "done"}
                            }),
                        })
                        .await
                        .unwrap();
                    BackendOutcome::Completed(CompletedTurn {
                        session_id,
                        final_message: "done".to_owned(),
                        usage: json!({"inputTokens": 2, "outputTokens": 1}),
                        snapshot_version: 1,
                        snapshot: json!({"version": 1}),
                        canonical_workspace: request.workspace.display().to_string(),
                    })
                }
                FakeBehavior::WaitForCancellation => {
                    cancellation.cancelled().await;
                    BackendOutcome::Cancelled {
                        session_id: Some(session_id),
                    }
                }
                FakeBehavior::SecondaryCleanupAfterCancellation => {
                    cancellation.cancelled().await;
                    BackendOutcome::Failed {
                        session_id: Some(session_id),
                        error: PublicError::new(
                            "cleanup_failed",
                            ErrorCategory::Cleanup,
                            "Agent cleanup did not finish cleanly.",
                            crate::error::RetryDisposition::Safe,
                        ),
                        completed: None,
                    }
                }
                FakeBehavior::CleanupFailedAfterCompleted => {
                    cancellation.cancelled().await;
                    BackendOutcome::Failed {
                        session_id: Some(session_id.clone()),
                        error: PublicError::new(
                            "cleanup_failed",
                            ErrorCategory::Cleanup,
                            "Agent cleanup did not finish cleanly.",
                            crate::error::RetryDisposition::Safe,
                        ),
                        completed: Some(CompletedTurn {
                            session_id,
                            final_message: "done".to_owned(),
                            usage: json!({"inputTokens": 2, "outputTokens": 1}),
                            snapshot_version: 1,
                            snapshot: json!({"version": 1}),
                            canonical_workspace: request.workspace.display().to_string(),
                        }),
                    }
                }
                FakeBehavior::FailBeforeAcceptanceWithSession => unreachable!(),
                FakeBehavior::WaitBeforeAcceptance => unreachable!(),
            }
        }
    }

    fn start_record(workspace: &std::path::Path) -> String {
        json!({
            "protocol": "smithers.nanocodex",
            "version": 1,
            "type": "turn.start",
            "commandId": "start-command",
            "requestId": "request-1",
            "data": {
                "prompt": "perform the task",
                "workspace": workspace,
                "auth": {
                    "mode": "api-key-env",
                    "environmentVariable": "TEST_PROVIDER_KEY"
                },
                "transport": {"kind": "websocket"},
                "options": {},
                "continuation": null
            }
        })
        .to_string()
            + "\n"
    }

    fn cancel_record(command_id: &str, session_id: &str, reason: &str) -> String {
        json!({
            "protocol": "smithers.nanocodex",
            "version": 1,
            "type": "turn.cancel",
            "commandId": command_id,
            "requestId": "request-1",
            "sessionId": session_id,
            "data": {"reason": reason}
        })
        .to_string()
            + "\n"
    }

    async fn read_json_line<R>(reader: &mut R) -> Value
    where
        R: AsyncBufRead + Unpin,
    {
        let mut line = String::new();
        reader.read_line(&mut line).await.unwrap();
        serde_json::from_str(&line).unwrap()
    }

    #[tokio::test]
    async fn bounded_reader_handles_fragmented_records() {
        let (mut write, read) = tokio::io::duplex(64);
        let task = tokio::spawn(async move {
            write.write_all(b"{\"a\":").await.unwrap();
            write.write_all(b"1}\n").await.unwrap();
        });
        let mut reader = RecordReader::new(read);
        assert_eq!(reader.read_record().await.unwrap().unwrap(), br#"{"a":1}"#);
        task.await.unwrap();
    }

    #[tokio::test]
    async fn bounded_reader_accepts_crlf_and_eof_without_lf() {
        let mut reader = RecordReader::new(&b"{\"a\":1}\r\n\r\n{\"b\":2}"[..]);
        assert_eq!(reader.read_record().await.unwrap().unwrap(), br#"{"a":1}"#);
        assert_eq!(reader.read_record().await.unwrap().unwrap(), br#"{"b":2}"#);
        assert!(reader.read_record().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn bounded_reader_leaves_a_cr_at_eof_in_the_record() {
        let mut reader = RecordReader::new(&b"{\"a\":1}\r"[..]);
        assert_eq!(reader.read_record().await.unwrap().unwrap(), b"{\"a\":1}\r");
    }

    #[tokio::test]
    async fn bounded_reader_counts_a_preceding_cr_toward_the_input_limit() {
        let mut accepted = vec![b'x'; MAX_INPUT_RECORD_BYTES];
        accepted.push(b'\n');
        let mut reader = RecordReader::new(accepted.as_slice());
        assert_eq!(
            reader.read_record().await.unwrap().unwrap().len(),
            MAX_INPUT_RECORD_BYTES
        );

        let mut rejected = vec![b'x'; MAX_INPUT_RECORD_BYTES];
        rejected.push(b'\r');
        rejected.push(b'\n');
        let mut reader = RecordReader::new(rejected.as_slice());
        let error = reader.read_record().await.unwrap_err();
        assert_eq!(error.code, "input_record_too_large");
    }

    #[tokio::test]
    async fn bounded_reader_rejects_records_over_the_input_limit() {
        let mut oversized = vec![b'x'; MAX_INPUT_RECORD_BYTES + 1];
        oversized.push(b'\n');
        let mut reader = RecordReader::new(oversized.as_slice());
        let error = reader.read_record().await.unwrap_err();
        assert_eq!(error.code, "input_record_too_large");
    }

    #[tokio::test]
    async fn bounded_reader_preserves_partial_bytes_when_a_read_future_is_cancelled() {
        let (mut write, read) = tokio::io::duplex(64);
        let mut reader = RecordReader::new(read);
        write.write_all(b"{\"a\":").await.unwrap();
        tokio::select! {
            biased;
            result = reader.read_record() => panic!("fragment unexpectedly completed: {result:?}"),
            () = tokio::task::yield_now() => {}
        }
        write.write_all(b"1}\n").await.unwrap();
        assert_eq!(reader.read_record().await.unwrap().unwrap(), br#"{"a":1}"#);
    }

    #[tokio::test]
    async fn writer_assigns_monotonic_sequence_numbers() {
        let (write, mut read) = tokio::io::duplex(4096);
        let failed = CancellationToken::new();
        let (sender, rx) = mpsc::unbounded_channel();
        let tx = OutputQueue {
            sender,
            event_slots: Arc::new(Semaphore::new(2)),
        };
        let task = tokio::spawn(writer_loop(write, rx, failed));
        tx.send_control(Outbound::new("hello", json!({})))
            .await
            .unwrap();
        tx.send_control(Outbound::new("process.failed", json!({})))
            .await
            .unwrap();
        drop(tx);
        task.await.unwrap().unwrap();
        let mut bytes = Vec::new();
        read.read_to_end(&mut bytes).await.unwrap();
        let lines = String::from_utf8(bytes).unwrap();
        let values = lines
            .lines()
            .map(|line| serde_json::from_str::<Value>(line).unwrap())
            .collect::<Vec<_>>();
        assert_eq!(values[0]["seq"], 1);
        assert_eq!(values[1]["seq"], 2);
    }

    #[tokio::test]
    async fn oversized_terminal_is_replaced_by_one_bounded_failure() {
        let (write, mut read) = tokio::io::duplex(4096);
        let failed = CancellationToken::new();
        let (sender, rx) = mpsc::unbounded_channel();
        let output = OutputQueue {
            sender,
            event_slots: Arc::new(Semaphore::new(2)),
        };
        let writer = tokio::spawn(writer_loop(write, rx, failed));
        let terminal = Outbound::new(
            "turn.completed",
            json!({"finalMessage": "x".repeat(MAX_OUTPUT_RECORD_BYTES)}),
        )
        .correlated("request-1", None, Some("session-1"));
        assert_eq!(
            send_terminal_record(&output, terminal).await,
            TerminalSend::Replaced
        );
        drop(output);
        writer.await.unwrap().unwrap();
        let mut bytes = Vec::new();
        read.read_to_end(&mut bytes).await.unwrap();
        let record: Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(record["type"], "turn.failed");
        assert_eq!(record["data"]["error"]["code"], "result_too_large");
    }

    #[test]
    fn cleanup_recovery_projection_keeps_a_maximum_snapshot_representable() {
        let payload = "x".repeat(MAX_SNAPSHOT_BYTES - 1024);
        let snapshot = json!({"payload": payload});
        assert!(serde_json::to_vec(&snapshot).unwrap().len() <= MAX_SNAPSHOT_BYTES);
        let completed = CompletedTurn {
            session_id: "session-1".to_owned(),
            final_message: "y".repeat(MAX_OUTPUT_RECORD_BYTES),
            usage: json!({"inputTokens": 1}),
            snapshot_version: 1,
            snapshot,
            canonical_workspace: "/workspace".to_owned(),
        };
        let recovery = recovery_json(completed);
        assert!(recovery.get("finalMessage").is_none());
        assert!(recovery.get("usage").is_none());
        let terminal = Outbound::new(
            "turn.failed",
            json!({
                "error": PublicError::new(
                    "cleanup_failed",
                    ErrorCategory::Cleanup,
                    "Agent cleanup did not finish cleanly.",
                    crate::error::RetryDisposition::Safe,
                ),
                "completed": recovery,
            }),
        )
        .correlated("request-1", None, Some("session-1"));
        assert!(outbound_fits_limit(
            &terminal,
            u64::MAX,
            MAX_OUTPUT_RECORD_BYTES
        ));
    }

    #[test]
    fn successful_projection_keeps_a_maximum_snapshot_and_duplicated_final_representable() {
        let final_message = "x".repeat(MAX_SNAPSHOT_BYTES - 2048);
        let snapshot = json!({"history": final_message});
        assert!(serde_json::to_vec(&snapshot).unwrap().len() <= MAX_SNAPSHOT_BYTES);
        let completed = CompletedTurn {
            session_id: "session-1".to_owned(),
            final_message: snapshot["history"].as_str().unwrap().to_owned(),
            usage: json!({
                "inputTokens": 0,
                "cachedInputTokens": 0,
                "cacheWriteInputTokens": 0,
                "outputTokens": 0,
                "reasoningOutputTokens": 0,
                "totalTokens": 0,
                "estimatedUsd": null,
                "costStatus": "usage_not_reported",
                "serviceTier": null,
            }),
            snapshot_version: 1,
            snapshot,
            canonical_workspace: "/workspace".to_owned(),
        };
        let terminal = Outbound::new("turn.completed", completed_json(completed)).correlated(
            "request-1",
            None,
            Some("session-1"),
        );
        assert!(outbound_fits_limit(
            &terminal,
            u64::MAX,
            MAX_OUTPUT_RECORD_BYTES
        ));
    }

    #[tokio::test]
    async fn stdout_failure_is_an_internal_process_failure() {
        let (_client_input, server_input) = tokio::io::duplex(64);
        let (server_output, client_output) = tokio::io::duplex(64);
        drop(client_output);
        let exit = tokio::time::timeout(
            std::time::Duration::from_secs(1),
            serve_with_backend(
                server_input,
                server_output,
                Arc::new(FakeBackend {
                    behavior: FakeBehavior::Complete,
                }),
                CancellationToken::new(),
            ),
        )
        .await
        .expect("writer failure did not settle");
        assert_eq!(exit, ServeExit::InternalOrCleanup);
    }

    #[tokio::test]
    async fn stdout_epipe_falls_back_while_exact_cancellation_is_hung() {
        let workspace = tempfile::tempdir().unwrap();
        let started = Arc::new(tokio::sync::Notify::new());
        let fallback_observed = Arc::new(AtomicBool::new(false));
        let (mut client_input, server_input) = tokio::io::duplex(8192);
        let (server_output, client_output) = tokio::io::duplex(64 * 1024);
        let server = tokio::spawn(serve_with_backend(
            server_input,
            server_output,
            Arc::new(ControlledCancellationBackend {
                exact_cancellation: Arc::new(HangingCancellation {
                    started: Arc::clone(&started),
                }),
                completion_release: None,
                fallback_observed: Arc::clone(&fallback_observed),
            }),
            CancellationToken::new(),
        ));
        let mut output = BufReader::new(client_output);
        assert_eq!(read_json_line(&mut output).await["type"], "hello");
        client_input
            .write_all(start_record(workspace.path()).as_bytes())
            .await
            .unwrap();
        assert_eq!(read_json_line(&mut output).await["type"], "turn.accepted");
        client_input
            .write_all(cancel_record("hung-at-epipe", "controlled-session", "timeout").as_bytes())
            .await
            .unwrap();
        started.notified().await;
        drop(output);
        client_input
            .write_all(cancel_record("force-epipe", "controlled-session", "ignored").as_bytes())
            .await
            .unwrap();

        assert_eq!(
            tokio::time::timeout(Duration::from_secs(1), server)
                .await
                .expect("EPIPE did not settle the server")
                .unwrap(),
            ServeExit::InternalOrCleanup
        );
        assert!(fallback_observed.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn blocked_stdout_does_not_block_input_or_exact_cancellation() {
        let workspace = tempfile::tempdir().unwrap();
        let observed = Arc::new(AtomicBool::new(false));
        let writer_state = Arc::new(GatedWriterState {
            records_written: AtomicUsize::new(0),
            blocked: AtomicBool::new(false),
            waker: AtomicWaker::new(),
            bytes: Mutex::new(Vec::new()),
        });
        let (mut client_input, server_input) = tokio::io::duplex(128 * 1024);
        let server = tokio::spawn(serve_with_backend(
            server_input,
            GatedWriter {
                state: Arc::clone(&writer_state),
            },
            Arc::new(FloodBackend {
                cancellation_observed: Arc::clone(&observed),
            }),
            CancellationToken::new(),
        ));
        client_input
            .write_all(start_record(workspace.path()).as_bytes())
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(1), async {
            while writer_state.records_written.load(Ordering::SeqCst) < 2 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("hello and turn.accepted were not written");
        assert!(writer_state.blocked.load(Ordering::SeqCst));

        let mut commands = String::new();
        for index in 0..64 {
            commands.push_str(
                &(json!({
                    "protocol": "smithers.nanocodex",
                    "version": 1,
                    "type": "turn.cancel",
                    "commandId": format!("pressure-rejection-{index}"),
                    "requestId": "wrong-request",
                    "sessionId": "flood-session",
                    "data": {}
                })
                .to_string()
                    + "\n"),
            );
        }
        commands.push_str(&cancel_record(
            "cancel-under-pressure",
            "flood-session",
            "timeout",
        ));
        client_input.write_all(commands.as_bytes()).await.unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            while !observed.load(Ordering::SeqCst) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("event pressure blocked exact cancellation");

        writer_state.release();
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(1), server)
                .await
                .expect("server did not settle after stdout resumed")
                .unwrap(),
            ServeExit::Cancelled
        );
        let bytes = writer_state.bytes.lock().unwrap().clone();
        let records = String::from_utf8(bytes).unwrap();
        assert!(records.contains("\"type\":\"command.accepted\""));
        assert!(records.contains("bridge_event_backpressure"));
        assert!(records.contains("\"type\":\"turn.cancelled\""));
    }

    #[tokio::test]
    async fn complete_turn_has_ordered_records_and_one_terminal() {
        let workspace = tempfile::tempdir().unwrap();
        let (mut client_input, server_input) = tokio::io::duplex(8192);
        let (server_output, mut client_output) = tokio::io::duplex(64 * 1024);
        let backend = Arc::new(FakeBackend {
            behavior: FakeBehavior::Complete,
        });
        let server = tokio::spawn(serve_with_backend(
            server_input,
            server_output,
            backend,
            CancellationToken::new(),
        ));
        client_input
            .write_all(start_record(workspace.path()).as_bytes())
            .await
            .unwrap();

        let mut output = String::new();
        client_output.read_to_string(&mut output).await.unwrap();
        assert_eq!(server.await.unwrap(), ServeExit::Success);
        let records = output
            .lines()
            .map(|line| serde_json::from_str::<Value>(line).unwrap())
            .collect::<Vec<_>>();
        let kinds = records
            .iter()
            .map(|record| record["type"].as_str().unwrap())
            .collect::<Vec<_>>();
        assert_eq!(
            kinds,
            ["hello", "turn.accepted", "agent.event", "turn.completed"]
        );
        assert_eq!(records.last().unwrap()["data"]["finalMessage"], "done");
        assert_eq!(
            records
                .iter()
                .filter(|record| {
                    matches!(
                        record["type"].as_str(),
                        Some(
                            "turn.completed" | "turn.failed" | "turn.cancelled" | "process.failed"
                        )
                    )
                })
                .count(),
            1
        );
        for (expected, record) in records.iter().enumerate() {
            assert_eq!(record["seq"], expected as u64 + 1);
        }
    }

    #[tokio::test]
    async fn correlated_cancel_is_acknowledged_then_terminal() {
        let workspace = tempfile::tempdir().unwrap();
        let (mut client_input, server_input) = tokio::io::duplex(8192);
        let (server_output, client_output) = tokio::io::duplex(64 * 1024);
        let backend = Arc::new(FakeBackend {
            behavior: FakeBehavior::WaitForCancellation,
        });
        let server = tokio::spawn(serve_with_backend(
            server_input,
            server_output,
            backend,
            CancellationToken::new(),
        ));
        let mut output = BufReader::new(client_output);
        assert_eq!(read_json_line(&mut output).await["type"], "hello");
        client_input
            .write_all(start_record(workspace.path()).as_bytes())
            .await
            .unwrap();
        let accepted = read_json_line(&mut output).await;
        assert_eq!(accepted["type"], "turn.accepted");
        let cancel = json!({
            "protocol": "smithers.nanocodex",
            "version": 1,
            "type": "turn.cancel",
            "commandId": "cancel-command",
            "requestId": "request-1",
            "sessionId": "fake-session",
            "data": {"reason": "test"}
        })
        .to_string()
            + "\n";
        client_input.write_all(cancel.as_bytes()).await.unwrap();
        let acknowledged = read_json_line(&mut output).await;
        assert_eq!(acknowledged["type"], "command.accepted");
        let terminal = read_json_line(&mut output).await;
        assert_eq!(terminal["type"], "turn.cancelled");
        assert_eq!(terminal["data"]["reason"], "test");
        assert_eq!(server.await.unwrap(), ServeExit::Cancelled);
    }

    #[tokio::test]
    async fn backend_completion_aborts_a_hung_exact_cancellation_before_terminal() {
        let workspace = tempfile::tempdir().unwrap();
        let started = Arc::new(tokio::sync::Notify::new());
        let release = Arc::new(tokio::sync::Notify::new());
        let fallback_observed = Arc::new(AtomicBool::new(false));
        let (mut client_input, server_input) = tokio::io::duplex(8192);
        let (server_output, client_output) = tokio::io::duplex(64 * 1024);
        let server = tokio::spawn(serve_with_backend(
            server_input,
            server_output,
            Arc::new(ControlledCancellationBackend {
                exact_cancellation: Arc::new(HangingCancellation {
                    started: Arc::clone(&started),
                }),
                completion_release: Some(Arc::clone(&release)),
                fallback_observed: Arc::clone(&fallback_observed),
            }),
            CancellationToken::new(),
        ));
        let mut output = BufReader::new(client_output);
        assert_eq!(read_json_line(&mut output).await["type"], "hello");
        client_input
            .write_all(start_record(workspace.path()).as_bytes())
            .await
            .unwrap();
        assert_eq!(read_json_line(&mut output).await["type"], "turn.accepted");
        client_input
            .write_all(cancel_record("hung-cancel", "controlled-session", "timeout").as_bytes())
            .await
            .unwrap();
        started.notified().await;
        release.notify_one();

        let rejected = read_json_line(&mut output).await;
        assert_eq!(rejected["type"], "command.rejected");
        assert_eq!(rejected["data"]["error"]["code"], "turn_not_cancellable");
        assert_eq!(read_json_line(&mut output).await["type"], "turn.completed");
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(1), server)
                .await
                .expect("hung cancellation blocked terminal finalization")
                .unwrap(),
            ServeExit::Success
        );
        assert!(!fallback_observed.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn panicked_exact_cancellation_fails_cleanly_and_uses_fallback() {
        let workspace = tempfile::tempdir().unwrap();
        let fallback_observed = Arc::new(AtomicBool::new(false));
        let (mut client_input, server_input) = tokio::io::duplex(8192);
        let (server_output, client_output) = tokio::io::duplex(64 * 1024);
        let server = tokio::spawn(serve_with_backend(
            server_input,
            server_output,
            Arc::new(ControlledCancellationBackend {
                exact_cancellation: Arc::new(PanickingCancellation),
                completion_release: None,
                fallback_observed: Arc::clone(&fallback_observed),
            }),
            CancellationToken::new(),
        ));
        let mut output = BufReader::new(client_output);
        assert_eq!(read_json_line(&mut output).await["type"], "hello");
        client_input
            .write_all(start_record(workspace.path()).as_bytes())
            .await
            .unwrap();
        assert_eq!(read_json_line(&mut output).await["type"], "turn.accepted");
        client_input
            .write_all(
                cancel_record("panicking-cancel", "controlled-session", "timeout").as_bytes(),
            )
            .await
            .unwrap();

        let rejected = read_json_line(&mut output).await;
        assert_eq!(rejected["type"], "command.rejected");
        assert_eq!(
            rejected["data"]["error"]["code"],
            "cancellation_task_failed"
        );
        let terminal = read_json_line(&mut output).await;
        assert_eq!(terminal["type"], "turn.failed");
        assert_eq!(
            terminal["data"]["error"]["code"],
            "cancellation_task_failed"
        );
        assert_eq!(server.await.unwrap(), ServeExit::InternalOrCleanup);
        assert!(fallback_observed.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn never_replying_exact_cancellation_times_out_and_uses_fallback() {
        let workspace = tempfile::tempdir().unwrap();
        let started = Arc::new(tokio::sync::Notify::new());
        let fallback_observed = Arc::new(AtomicBool::new(false));
        let (mut client_input, server_input) = tokio::io::duplex(8192);
        let (server_output, client_output) = tokio::io::duplex(64 * 1024);
        let server = tokio::spawn(serve_with_backend(
            server_input,
            server_output,
            Arc::new(ControlledCancellationBackend {
                exact_cancellation: Arc::new(HangingCancellation {
                    started: Arc::clone(&started),
                }),
                completion_release: None,
                fallback_observed: Arc::clone(&fallback_observed),
            }),
            CancellationToken::new(),
        ));
        let mut output = BufReader::new(client_output);
        assert_eq!(read_json_line(&mut output).await["type"], "hello");
        client_input
            .write_all(start_record(workspace.path()).as_bytes())
            .await
            .unwrap();
        assert_eq!(read_json_line(&mut output).await["type"], "turn.accepted");
        client_input
            .write_all(
                cancel_record("timed-out-cancel", "controlled-session", "timeout").as_bytes(),
            )
            .await
            .unwrap();
        started.notified().await;

        let rejected = tokio::time::timeout(Duration::from_secs(2), read_json_line(&mut output))
            .await
            .expect("never-replying cancellation was not supervised");
        assert_eq!(rejected["type"], "command.rejected");
        assert_eq!(rejected["data"]["error"]["code"], "cancellation_timed_out");
        let terminal = read_json_line(&mut output).await;
        assert_eq!(terminal["type"], "turn.failed");
        assert_eq!(terminal["data"]["error"]["code"], "cancellation_timed_out");
        assert_eq!(server.await.unwrap(), ServeExit::InternalOrCleanup);
        assert!(fallback_observed.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn external_shutdown_falls_back_while_exact_cancellation_is_hung() {
        let workspace = tempfile::tempdir().unwrap();
        let started = Arc::new(tokio::sync::Notify::new());
        let fallback_observed = Arc::new(AtomicBool::new(false));
        let shutdown = CancellationToken::new();
        let (mut client_input, server_input) = tokio::io::duplex(8192);
        let (server_output, client_output) = tokio::io::duplex(64 * 1024);
        let server = tokio::spawn(serve_with_backend(
            server_input,
            server_output,
            Arc::new(ControlledCancellationBackend {
                exact_cancellation: Arc::new(HangingCancellation {
                    started: Arc::clone(&started),
                }),
                completion_release: None,
                fallback_observed: Arc::clone(&fallback_observed),
            }),
            shutdown.clone(),
        ));
        let mut output = BufReader::new(client_output);
        assert_eq!(read_json_line(&mut output).await["type"], "hello");
        client_input
            .write_all(start_record(workspace.path()).as_bytes())
            .await
            .unwrap();
        assert_eq!(read_json_line(&mut output).await["type"], "turn.accepted");
        client_input
            .write_all(cancel_record("hung-at-signal", "controlled-session", "timeout").as_bytes())
            .await
            .unwrap();
        started.notified().await;
        shutdown.cancel();

        let rejected = read_json_line(&mut output).await;
        assert_eq!(rejected["type"], "command.rejected");
        assert_eq!(rejected["data"]["error"]["code"], "turn_not_cancellable");
        let terminal = read_json_line(&mut output).await;
        assert_eq!(terminal["type"], "turn.cancelled");
        assert_eq!(terminal["data"]["reason"], "external_signal");
        assert_eq!(server.await.unwrap(), ServeExit::Cancelled);
        assert!(fallback_observed.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn fragmented_post_start_cancel_survives_interleaved_backend_notice() {
        let workspace = tempfile::tempdir().unwrap();
        let emit_event = Arc::new(tokio::sync::Notify::new());
        let (mut client_input, server_input) = tokio::io::duplex(8192);
        let (server_output, client_output) = tokio::io::duplex(64 * 1024);
        let server = tokio::spawn(serve_with_backend(
            server_input,
            server_output,
            Arc::new(FragmentBackend {
                emit_event: Arc::clone(&emit_event),
            }),
            CancellationToken::new(),
        ));
        let mut output = BufReader::new(client_output);
        assert_eq!(read_json_line(&mut output).await["type"], "hello");
        client_input
            .write_all(start_record(workspace.path()).as_bytes())
            .await
            .unwrap();
        assert_eq!(read_json_line(&mut output).await["type"], "turn.accepted");
        let cancel = json!({
            "protocol": "smithers.nanocodex",
            "version": 1,
            "type": "turn.cancel",
            "commandId": "fragmented-cancel",
            "requestId": "request-1",
            "sessionId": "fragment-session",
            "data": {"reason": "fragment-test"}
        })
        .to_string()
            + "\n";
        let split = cancel.len() / 2;
        client_input
            .write_all(&cancel.as_bytes()[..split])
            .await
            .unwrap();
        tokio::task::yield_now().await;
        emit_event.notify_one();
        assert_eq!(read_json_line(&mut output).await["type"], "agent.event");
        client_input
            .write_all(&cancel.as_bytes()[split..])
            .await
            .unwrap();
        assert_eq!(
            read_json_line(&mut output).await["type"],
            "command.accepted"
        );
        let terminal = read_json_line(&mut output).await;
        assert_eq!(terminal["type"], "turn.cancelled");
        assert_eq!(terminal["data"]["reason"], "fragment-test");
        assert_eq!(server.await.unwrap(), ServeExit::Cancelled);
    }

    #[tokio::test]
    async fn accepted_cancellation_is_not_reclassified_by_secondary_cleanup_failure() {
        let workspace = tempfile::tempdir().unwrap();
        let (mut client_input, server_input) = tokio::io::duplex(8192);
        let (server_output, client_output) = tokio::io::duplex(64 * 1024);
        let server = tokio::spawn(serve_with_backend(
            server_input,
            server_output,
            Arc::new(FakeBackend {
                behavior: FakeBehavior::SecondaryCleanupAfterCancellation,
            }),
            CancellationToken::new(),
        ));
        let mut output = BufReader::new(client_output);
        assert_eq!(read_json_line(&mut output).await["type"], "hello");
        client_input
            .write_all(start_record(workspace.path()).as_bytes())
            .await
            .unwrap();
        assert_eq!(read_json_line(&mut output).await["type"], "turn.accepted");
        let cancel = json!({
            "protocol": "smithers.nanocodex",
            "version": 1,
            "type": "turn.cancel",
            "commandId": "cancel-before-cleanup-failure",
            "requestId": "request-1",
            "sessionId": "fake-session",
            "data": {"reason": "timeout"}
        })
        .to_string()
            + "\n";
        client_input.write_all(cancel.as_bytes()).await.unwrap();
        assert_eq!(
            read_json_line(&mut output).await["type"],
            "command.accepted"
        );
        let terminal = read_json_line(&mut output).await;
        assert_eq!(terminal["type"], "turn.cancelled");
        assert_eq!(terminal["data"]["reason"], "timeout");
        assert_eq!(server.await.unwrap(), ServeExit::Cancelled);
    }

    #[tokio::test]
    async fn completed_cleanup_failure_is_not_reclassified_by_a_late_cancellation() {
        let workspace = tempfile::tempdir().unwrap();
        let (mut client_input, server_input) = tokio::io::duplex(8192);
        let (server_output, client_output) = tokio::io::duplex(64 * 1024);
        let server = tokio::spawn(serve_with_backend(
            server_input,
            server_output,
            Arc::new(FakeBackend {
                behavior: FakeBehavior::CleanupFailedAfterCompleted,
            }),
            CancellationToken::new(),
        ));
        let mut output = BufReader::new(client_output);
        assert_eq!(read_json_line(&mut output).await["type"], "hello");
        client_input
            .write_all(start_record(workspace.path()).as_bytes())
            .await
            .unwrap();
        assert_eq!(read_json_line(&mut output).await["type"], "turn.accepted");
        drop(client_input);
        let terminal = read_json_line(&mut output).await;
        assert_eq!(terminal["type"], "turn.failed");
        assert_eq!(terminal["data"]["error"]["code"], "cleanup_failed");
        assert_eq!(terminal["data"]["completed"]["snapshotVersion"], 1);
        assert!(terminal["data"]["completed"]["snapshot"].is_object());
        assert_eq!(server.await.unwrap(), ServeExit::InternalOrCleanup);
    }

    #[tokio::test]
    async fn running_command_state_is_bounded_by_the_advertised_limit() {
        let workspace = tempfile::tempdir().unwrap();
        let (mut client_input, server_input) = tokio::io::duplex(64 * 1024);
        let (server_output, client_output) = tokio::io::duplex(64 * 1024);
        let server = tokio::spawn(serve_with_backend(
            server_input,
            server_output,
            Arc::new(FakeBackend {
                behavior: FakeBehavior::WaitForCancellation,
            }),
            CancellationToken::new(),
        ));
        let mut output = BufReader::new(client_output);
        assert_eq!(read_json_line(&mut output).await["type"], "hello");
        client_input
            .write_all(start_record(workspace.path()).as_bytes())
            .await
            .unwrap();
        assert_eq!(read_json_line(&mut output).await["type"], "turn.accepted");

        for index in 1..MAX_COMMAND_RECORDS {
            let command = json!({
                "protocol": "smithers.nanocodex",
                "version": 1,
                "type": "turn.cancel",
                "commandId": format!("mismatch-{index}"),
                "requestId": "wrong-request",
                "sessionId": "fake-session",
                "data": {"reason": "ignored"}
            })
            .to_string()
                + "\n";
            client_input.write_all(command.as_bytes()).await.unwrap();
            assert_eq!(
                read_json_line(&mut output).await["data"]["error"]["code"],
                "correlation_mismatch"
            );
        }

        let overflow = json!({
            "protocol": "smithers.nanocodex",
            "version": 1,
            "type": "turn.cancel",
            "commandId": "one-command-too-many",
            "requestId": "wrong-request",
            "sessionId": "fake-session",
            "data": {"reason": "ignored"}
        })
        .to_string()
            + "\n";
        client_input.write_all(overflow.as_bytes()).await.unwrap();
        let terminal = read_json_line(&mut output).await;
        assert_eq!(terminal["type"], "turn.failed");
        assert_eq!(terminal["data"]["error"]["code"], "too_many_commands");
        assert_eq!(server.await.unwrap(), ServeExit::ProtocolOrConfig);
    }

    #[tokio::test]
    async fn duplicate_ids_still_consume_the_physical_command_record_limit() {
        let workspace = tempfile::tempdir().unwrap();
        let (mut client_input, server_input) = tokio::io::duplex(128 * 1024);
        let (server_output, client_output) = tokio::io::duplex(512 * 1024);
        let server = tokio::spawn(serve_with_backend(
            server_input,
            server_output,
            Arc::new(FakeBackend {
                behavior: FakeBehavior::WaitForCancellation,
            }),
            CancellationToken::new(),
        ));
        let mut output = BufReader::new(client_output);
        assert_eq!(read_json_line(&mut output).await["type"], "hello");
        client_input
            .write_all(start_record(workspace.path()).as_bytes())
            .await
            .unwrap();
        assert_eq!(read_json_line(&mut output).await["type"], "turn.accepted");

        let mut input = String::new();
        let duplicate = cancel_record("start-command", "fake-session", "ignored");
        for _ in 1..MAX_COMMAND_RECORDS {
            input.push_str(&duplicate);
        }
        input.push_str(&duplicate);
        client_input.write_all(input.as_bytes()).await.unwrap();
        drop(client_input);

        let mut remainder = String::new();
        output.read_to_string(&mut remainder).await.unwrap();
        assert_eq!(server.await.unwrap(), ServeExit::ProtocolOrConfig);
        let records = remainder
            .lines()
            .map(|line| serde_json::from_str::<Value>(line).unwrap())
            .collect::<Vec<_>>();
        assert_eq!(
            records
                .iter()
                .filter(|record| record["type"] == "command.rejected")
                .count(),
            MAX_COMMAND_RECORDS - 1
        );
        let terminal = records.last().unwrap();
        assert_eq!(terminal["type"], "turn.failed");
        assert_eq!(terminal["data"]["error"]["code"], "too_many_commands");
    }

    #[tokio::test]
    async fn cancellation_during_finalization_is_rejected_before_completion() {
        let workspace = tempfile::tempdir().unwrap();
        let cancellation_invoked = Arc::new(tokio::sync::Notify::new());
        let (mut client_input, server_input) = tokio::io::duplex(8192);
        let (server_output, client_output) = tokio::io::duplex(64 * 1024);
        let server = tokio::spawn(serve_with_backend(
            server_input,
            server_output,
            Arc::new(LateCompletionBackend {
                cancellation_invoked,
            }),
            CancellationToken::new(),
        ));
        let mut output = BufReader::new(client_output);
        assert_eq!(read_json_line(&mut output).await["type"], "hello");
        client_input
            .write_all(start_record(workspace.path()).as_bytes())
            .await
            .unwrap();
        assert_eq!(read_json_line(&mut output).await["type"], "turn.accepted");
        let cancel = json!({
            "protocol": "smithers.nanocodex",
            "version": 1,
            "type": "turn.cancel",
            "commandId": "late-cancel",
            "requestId": "request-1",
            "sessionId": "fake-session",
            "data": {"reason": "timeout"}
        })
        .to_string()
            + "\n";
        client_input.write_all(cancel.as_bytes()).await.unwrap();
        let rejected = read_json_line(&mut output).await;
        assert_eq!(rejected["type"], "command.rejected");
        assert_eq!(rejected["data"]["error"]["code"], "turn_not_cancellable");
        assert_eq!(read_json_line(&mut output).await["type"], "turn.completed");
        assert_eq!(server.await.unwrap(), ServeExit::Success);
    }

    #[tokio::test]
    async fn a_buffered_cancel_is_always_rejected_when_backend_completion_wins() {
        for index in 0..64 {
            let workspace = tempfile::tempdir().unwrap();
            let release = Arc::new(tokio::sync::Notify::new());
            let calls = Arc::new(AtomicUsize::new(0));
            let (mut client_input, server_input) = tokio::io::duplex(8192);
            let (server_output, client_output) = tokio::io::duplex(64 * 1024);
            let server = tokio::spawn(serve_with_backend(
                server_input,
                server_output,
                Arc::new(CompletionRaceBackend {
                    release: Arc::clone(&release),
                    cancellation_calls: calls,
                }),
                CancellationToken::new(),
            ));
            let mut output = BufReader::new(client_output);
            assert_eq!(read_json_line(&mut output).await["type"], "hello");
            client_input
                .write_all(start_record(workspace.path()).as_bytes())
                .await
                .unwrap();
            assert_eq!(read_json_line(&mut output).await["type"], "turn.accepted");
            let cancel = json!({
                "protocol": "smithers.nanocodex",
                "version": 1,
                "type": "turn.cancel",
                "commandId": format!("race-cancel-{index}"),
                "requestId": "request-1",
                "sessionId": "finalizing-session",
                "data": {"reason": "timeout"}
            })
            .to_string()
                + "\n";
            client_input.write_all(cancel.as_bytes()).await.unwrap();
            release.notify_one();

            let mut remainder = String::new();
            output.read_to_string(&mut remainder).await.unwrap();
            let kinds = remainder
                .lines()
                .map(|line| serde_json::from_str::<Value>(line).unwrap()["type"].clone())
                .collect::<Vec<_>>();
            assert_eq!(
                kinds,
                [
                    Value::String("command.rejected".to_owned()),
                    Value::String("turn.completed".to_owned())
                ],
                "iteration {index}",
            );
            assert_eq!(server.await.unwrap(), ServeExit::Success);
        }
    }

    #[tokio::test]
    async fn eof_falls_back_after_one_rejected_exact_cancellation() {
        let workspace = tempfile::tempdir().unwrap();
        let release = Arc::new(tokio::sync::Notify::new());
        let calls = Arc::new(AtomicUsize::new(0));
        let (mut client_input, server_input) = tokio::io::duplex(8192);
        let (server_output, client_output) = tokio::io::duplex(64 * 1024);
        let server = tokio::spawn(serve_with_backend(
            server_input,
            server_output,
            Arc::new(CompletionRaceBackend {
                release: Arc::clone(&release),
                cancellation_calls: Arc::clone(&calls),
            }),
            CancellationToken::new(),
        ));
        let mut output = BufReader::new(client_output);
        assert_eq!(read_json_line(&mut output).await["type"], "hello");
        client_input
            .write_all(start_record(workspace.path()).as_bytes())
            .await
            .unwrap();
        assert_eq!(read_json_line(&mut output).await["type"], "turn.accepted");
        for command_id in ["late-cancel-1", "late-cancel-2"] {
            let cancel = json!({
                "protocol": "smithers.nanocodex",
                "version": 1,
                "type": "turn.cancel",
                "commandId": command_id,
                "requestId": "request-1",
                "sessionId": "finalizing-session",
                "data": {"reason": "timeout"}
            })
            .to_string()
                + "\n";
            client_input.write_all(cancel.as_bytes()).await.unwrap();
            assert_eq!(
                read_json_line(&mut output).await["type"],
                "command.rejected"
            );
        }
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        drop(client_input);
        let terminal = read_json_line(&mut output).await;
        assert_eq!(terminal["type"], "turn.cancelled");
        assert_eq!(terminal["data"]["reason"], "stdin_eof");
        assert_eq!(calls.load(Ordering::SeqCst), 101);
        assert_eq!(server.await.unwrap(), ServeExit::Cancelled);
    }

    #[tokio::test]
    async fn cancellation_can_be_latched_before_session_acceptance() {
        let workspace = tempfile::tempdir().unwrap();
        let (mut client_input, server_input) = tokio::io::duplex(8192);
        let (server_output, client_output) = tokio::io::duplex(64 * 1024);
        let backend = Arc::new(FakeBackend {
            behavior: FakeBehavior::WaitBeforeAcceptance,
        });
        let server = tokio::spawn(serve_with_backend(
            server_input,
            server_output,
            backend,
            CancellationToken::new(),
        ));
        let mut output = BufReader::new(client_output);
        assert_eq!(read_json_line(&mut output).await["type"], "hello");
        client_input
            .write_all(start_record(workspace.path()).as_bytes())
            .await
            .unwrap();
        let cancel = json!({
            "protocol": "smithers.nanocodex",
            "version": 1,
            "type": "turn.cancel",
            "commandId": "cancel-before-acceptance",
            "requestId": "request-1",
            "data": {"reason": "timeout"}
        })
        .to_string()
            + "\n";
        client_input.write_all(cancel.as_bytes()).await.unwrap();

        let acknowledged = read_json_line(&mut output).await;
        assert_eq!(acknowledged["type"], "command.accepted");
        assert!(acknowledged.get("sessionId").is_none());
        let terminal = read_json_line(&mut output).await;
        assert_eq!(terminal["type"], "process.failed");
        assert_eq!(
            terminal["data"]["error"]["code"],
            "cancelled_before_acceptance"
        );
        assert_eq!(server.await.unwrap(), ServeExit::Cancelled);
    }

    #[tokio::test]
    async fn failure_before_acceptance_never_exposes_an_internal_session() {
        let workspace = tempfile::tempdir().unwrap();
        let (mut client_input, server_input) = tokio::io::duplex(8192);
        let (server_output, client_output) = tokio::io::duplex(64 * 1024);
        let server = tokio::spawn(serve_with_backend(
            server_input,
            server_output,
            Arc::new(FakeBackend {
                behavior: FakeBehavior::FailBeforeAcceptanceWithSession,
            }),
            CancellationToken::new(),
        ));
        let mut output = BufReader::new(client_output);
        assert_eq!(read_json_line(&mut output).await["type"], "hello");
        client_input
            .write_all(start_record(workspace.path()).as_bytes())
            .await
            .unwrap();

        let terminal = read_json_line(&mut output).await;
        assert_eq!(terminal["type"], "process.failed");
        assert!(terminal.get("sessionId").is_none());
        assert_eq!(terminal["data"]["error"]["code"], "prompt_rejected");
        assert_eq!(server.await.unwrap(), ServeExit::TurnFailure);
    }

    #[tokio::test]
    async fn malformed_record_after_acceptance_is_a_single_turn_failure() {
        let workspace = tempfile::tempdir().unwrap();
        let (mut client_input, server_input) = tokio::io::duplex(8192);
        let (server_output, client_output) = tokio::io::duplex(64 * 1024);
        let backend = Arc::new(FakeBackend {
            behavior: FakeBehavior::WaitForCancellation,
        });
        let server = tokio::spawn(serve_with_backend(
            server_input,
            server_output,
            backend,
            CancellationToken::new(),
        ));
        let mut output = BufReader::new(client_output);
        assert_eq!(read_json_line(&mut output).await["type"], "hello");
        client_input
            .write_all(start_record(workspace.path()).as_bytes())
            .await
            .unwrap();
        assert_eq!(read_json_line(&mut output).await["type"], "turn.accepted");
        client_input.write_all(b"{not-json\n").await.unwrap();

        let terminal = read_json_line(&mut output).await;
        assert_eq!(terminal["type"], "turn.failed");
        assert_eq!(terminal["data"]["error"]["code"], "invalid_json");
        assert_eq!(server.await.unwrap(), ServeExit::ProtocolOrConfig);
    }

    #[tokio::test]
    async fn stdin_eof_cancels_without_busy_loop() {
        let workspace = tempfile::tempdir().unwrap();
        let (mut client_input, server_input) = tokio::io::duplex(8192);
        let (server_output, mut client_output) = tokio::io::duplex(64 * 1024);
        let backend = Arc::new(FakeBackend {
            behavior: FakeBehavior::WaitForCancellation,
        });
        let server = tokio::spawn(serve_with_backend(
            server_input,
            server_output,
            backend,
            CancellationToken::new(),
        ));
        client_input
            .write_all(start_record(workspace.path()).as_bytes())
            .await
            .unwrap();
        drop(client_input);
        let mut output = String::new();
        tokio::time::timeout(
            std::time::Duration::from_secs(1),
            client_output.read_to_string(&mut output),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(server.await.unwrap(), ServeExit::Cancelled);
        let terminal_count = output
            .lines()
            .filter(|line| {
                let value: Value = serde_json::from_str(line).unwrap();
                matches!(
                    value["type"].as_str(),
                    Some("turn.cancelled" | "process.failed")
                )
            })
            .count();
        assert_eq!(terminal_count, 1);
    }
}
