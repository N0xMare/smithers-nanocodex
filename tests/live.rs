use std::{
    fs,
    io::Write,
    path::PathBuf,
    process::{Command, Stdio},
    sync::mpsc::{self, RecvTimeoutError},
    thread,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use serde_json::{Value, json};

struct LiveTurn {
    pid: u32,
    records: Vec<Value>,
}

const LIVE_TURN_TIMEOUT: Duration = Duration::from_secs(180);
const LIVE_CLEANUP_TIMEOUT: Duration = Duration::from_secs(10);

fn run_live_turn(start: Value) -> LiveTurn {
    let request_id = start["requestId"]
        .as_str()
        .expect("live start omitted requestId")
        .to_owned();
    let command_id = start["commandId"]
        .as_str()
        .expect("live start omitted commandId")
        .to_owned();
    let mut child = Command::new(env!("CARGO_BIN_EXE_smithers-nanocodex"))
        .args(["serve", "--protocol-version", "1"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("live bridge failed to start");
    let pid = child.id();
    let mut open_stdin = child.stdin.take().expect("live stdin was not piped");
    writeln!(open_stdin, "{}", serde_json::to_string(&start).unwrap()).unwrap();
    let (output_sender, output_receiver) = mpsc::channel();
    thread::spawn(move || {
        let _ = output_sender.send(child.wait_with_output());
    });
    let output = match output_receiver.recv_timeout(LIVE_TURN_TIMEOUT) {
        Ok(output) => output.expect("live bridge output collection failed"),
        Err(RecvTimeoutError::Timeout) => {
            drop(open_stdin);
            let pid_text = pid.to_string();
            let _ = Command::new("kill").args(["-TERM", &pid_text]).status();
            if output_receiver.recv_timeout(LIVE_CLEANUP_TIMEOUT).is_err() {
                let _ = Command::new("kill").args(["-KILL", &pid_text]).status();
                let _ = output_receiver.recv_timeout(LIVE_CLEANUP_TIMEOUT);
            }
            panic!("live bridge exceeded its wall-clock timeout");
        }
        Err(RecvTimeoutError::Disconnected) => {
            panic!("live bridge output worker stopped unexpectedly")
        }
    };
    drop(open_stdin);
    let records = output
        .stdout
        .split(|byte| *byte == b'\n')
        .filter(|line| !line.is_empty())
        .map(|line| serde_json::from_slice::<Value>(line).expect("live output was not JSONL"))
        .collect::<Vec<_>>();
    let hello = records
        .first()
        .expect("live bridge omitted its hello record");
    assert_eq!(hello["type"], "hello");
    assert_eq!(hello["data"]["bridgeVersion"], env!("CARGO_PKG_VERSION"));
    assert_eq!(hello["data"]["nanocodexVersion"], "0.5.0");
    assert_eq!(hello["data"]["target"], env!("SMITHERS_NANOCODEX_TARGET"));
    assert!(
        output.status.success(),
        "live bridge process exited unsuccessfully"
    );
    assert!(
        records.last().and_then(|record| record["type"].as_str()) == Some("turn.completed"),
        "live bridge omitted its completion record"
    );
    assert!(
        records
            .iter()
            .enumerate()
            .all(|(index, record)| record["seq"].as_u64() == Some(index as u64 + 1)),
        "live bridge sequence was not contiguous"
    );
    assert!(
        records
            .iter()
            .filter(|record| record["type"] != "hello")
            .all(|record| record["requestId"].as_str() == Some(&request_id)),
        "live bridge response correlation was invalid"
    );
    assert!(
        records
            .iter()
            .filter(|record| record.get("commandId").is_some())
            .all(|record| record["commandId"].as_str() == Some(&command_id)),
        "live bridge command correlation was invalid"
    );
    assert!(
        records
            .iter()
            .filter(|record| record["type"] == "turn.accepted")
            .count()
            == 1,
        "live bridge did not accept exactly one turn"
    );
    let encoded = format!(
        "{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let encoded_lower = encoded.to_ascii_lowercase();
    assert!(
        ![
            "access_token",
            "refresh_token",
            "openai_api_key",
            "authorization: bearer",
        ]
        .iter()
        .any(|sentinel| encoded_lower.contains(sentinel)),
        "live bridge output exposed credential-shaped data"
    );
    assert!(
        credential_values()
            .iter()
            .filter(|secret| secret.len() >= 8)
            .all(|secret| !encoded.contains(secret)),
        "live bridge output exposed a configured credential value"
    );
    LiveTurn { pid, records }
}

fn credential_values() -> Vec<String> {
    let mut secrets = Vec::new();
    if let Ok(api_key) = std::env::var("OPENAI_API_KEY") {
        secrets.push(api_key);
    }
    if let Some(path) = managed_auth_file()
        && let Ok(bytes) = fs::read(path)
        && let Ok(value) = serde_json::from_slice::<Value>(&bytes)
    {
        collect_secret_values(&value, &mut secrets);
    }
    secrets.sort();
    secrets.dedup();
    secrets
}

fn managed_auth_file() -> Option<PathBuf> {
    if let Some(path) = std::env::var_os("NANOCODEX_AUTH_FILE") {
        return Some(PathBuf::from(path));
    }
    if let Some(path) = std::env::var_os("CODEX_HOME") {
        return Some(PathBuf::from(path).join("auth.json"));
    }
    std::env::var_os("HOME").map(|home| PathBuf::from(home).join(".codex/auth.json"))
}

fn collect_secret_values(value: &Value, secrets: &mut Vec<String>) {
    match value {
        Value::Object(values) => {
            for (key, value) in values {
                let normalized = key.to_ascii_lowercase();
                if (normalized.contains("token") || normalized.contains("api_key"))
                    && let Some(secret) = value.as_str()
                {
                    secrets.push(secret.to_owned());
                }
                collect_secret_values(value, secrets);
            }
        }
        Value::Array(values) => {
            for value in values {
                collect_secret_values(value, secrets);
            }
        }
        _ => {}
    }
}

fn terminal(turn: &LiveTurn) -> &Value {
    turn.records
        .last()
        .expect("live bridge produced no terminal record")
}

fn assert_completed(turn: &LiveTurn, canonical: &std::path::Path, expected: &str) {
    let completed = terminal(turn);
    assert!(
        completed["data"]["canonicalWorkspace"].as_str()
            == Some(canonical.to_string_lossy().as_ref()),
        "live turn returned an unexpected canonical workspace"
    );
    assert!(
        completed["data"]["finalMessage"].as_str().map(str::trim) == Some(expected),
        "live turn returned an unexpected final message"
    );
    let usage = &completed["data"]["usage"];
    let input = usage["inputTokens"]
        .as_u64()
        .expect("live usage omitted inputTokens");
    let output = usage["outputTokens"]
        .as_u64()
        .expect("live usage omitted outputTokens");
    let total = usage["totalTokens"]
        .as_u64()
        .expect("live usage omitted totalTokens");
    let cached = usage["cachedInputTokens"]
        .as_u64()
        .expect("live usage omitted cachedInputTokens");
    let cache_write = usage["cacheWriteInputTokens"]
        .as_u64()
        .expect("live usage omitted cacheWriteInputTokens");
    let reasoning = usage["reasoningOutputTokens"]
        .as_u64()
        .expect("live usage omitted reasoningOutputTokens");
    assert!(
        output > 0
            && total == input.saturating_add(output)
            && cached.saturating_add(cache_write) <= input
            && reasoning <= output,
        "live usage counters were inconsistent"
    );
    assert!(
        completed["data"]["snapshotVersion"].as_u64() == Some(1)
            && completed["data"]["snapshot"].is_object(),
        "live turn omitted its versioned snapshot"
    );
}

fn unique_token(prefix: &str) -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock predates the Unix epoch")
        .as_nanos();
    format!("SMITHERS_NANOCODEX_{prefix}_{}_{nanos}", std::process::id())
}

fn start_record(
    workspace: &std::path::Path,
    auth: Value,
    prompt: &str,
    request_id: &str,
    continuation: Value,
) -> Value {
    json!({
        "protocol": "smithers.nanocodex",
        "version": 1,
        "type": "turn.start",
        "commandId": format!("start-{request_id}"),
        "requestId": request_id,
        "data": {
            "prompt": prompt,
            "workspace": workspace,
            "auth": auth,
            "transport": {"kind": "websocket"},
            "options": {},
            "continuation": continuation,
        }
    })
}

#[test]
#[ignore = "requires explicit live opt-in and OPENAI_API_KEY"]
fn live_api_key_turn_and_fresh_process_resume() {
    assert_eq!(
        std::env::var("SMITHERS_NANOCODEX_LIVE_API_KEY").as_deref(),
        Ok("1"),
        "set SMITHERS_NANOCODEX_LIVE_API_KEY=1 to authorize live provider calls"
    );
    assert!(
        std::env::var_os("OPENAI_API_KEY").is_some(),
        "OPENAI_API_KEY is required"
    );
    let workspace = tempfile::tempdir().unwrap();
    let canonical = std::fs::canonicalize(workspace.path()).unwrap();
    let auth = json!({
        "mode": "api-key-env",
        "environmentVariable": "OPENAI_API_KEY",
    });
    let nonce = unique_token("API_MEMORY");
    let acknowledgement = unique_token("API_STORED");
    let first = run_live_turn(start_record(
        &canonical,
        auth.clone(),
        &format!(
            "Remember the nonce {nonce} for the next turn. Reply with exactly {acknowledgement} and no other text. Do not use tools."
        ),
        "live-api-first",
        Value::Null,
    ));
    assert_completed(&first, &canonical, &acknowledgement);
    let snapshot = terminal(&first)["data"]["snapshot"].clone();
    let second = run_live_turn(start_record(
        &canonical,
        auth,
        "Reply with exactly the nonce I asked you to remember in the previous turn and no other text. Do not use tools.",
        "live-api-resume",
        json!({"mode": "resume", "snapshot": snapshot}),
    ));
    assert_completed(&second, &canonical, &nonce);
    assert_ne!(
        first.pid, second.pid,
        "live resume reused the bridge process"
    );
}

#[test]
#[ignore = "requires explicit live opt-in and managed ChatGPT authentication"]
fn live_managed_chatgpt_turn_and_fresh_process_resume() {
    assert_eq!(
        std::env::var("SMITHERS_NANOCODEX_LIVE_CHATGPT").as_deref(),
        Ok("1"),
        "set SMITHERS_NANOCODEX_LIVE_CHATGPT=1 to authorize managed-auth live calls"
    );
    let workspace = tempfile::tempdir().unwrap();
    let canonical = std::fs::canonicalize(workspace.path()).unwrap();
    let auth = json!({"mode": "chatgpt"});
    let nonce = unique_token("CHATGPT_MEMORY");
    let acknowledgement = unique_token("CHATGPT_STORED");
    let first = run_live_turn(start_record(
        &canonical,
        auth.clone(),
        &format!(
            "Remember the nonce {nonce} for the next turn. Reply with exactly {acknowledgement} and no other text. Do not use tools."
        ),
        "live-chatgpt-first",
        Value::Null,
    ));
    assert_completed(&first, &canonical, &acknowledgement);
    let snapshot = terminal(&first)["data"]["snapshot"].clone();
    let second = run_live_turn(start_record(
        &canonical,
        auth,
        "Reply with exactly the nonce I asked you to remember in the previous turn and no other text. Do not use tools.",
        "live-chatgpt-resume",
        json!({"mode": "resume", "snapshot": snapshot}),
    ));
    assert_completed(&second, &canonical, &nonce);
    assert_ne!(
        first.pid, second.pid,
        "live resume reused the bridge process"
    );
}
