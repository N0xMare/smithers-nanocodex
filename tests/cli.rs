use std::{
    io::{BufRead, BufReader, Write},
    process::{Command, Stdio},
    thread,
    time::{Duration, Instant},
};

use serde_json::Value;

fn binary() -> Command {
    Command::new(env!("CARGO_BIN_EXE_smithers-nanocodex"))
}

#[test]
fn capabilities_command_is_machine_readable_and_side_effect_free() {
    let output = binary()
        .args(["capabilities", "--json"])
        .output()
        .expect("capabilities command failed to start");
    assert!(output.status.success());
    assert!(output.stderr.is_empty());
    let value: Value = serde_json::from_slice(&output.stdout).expect("capabilities were not JSON");
    assert_eq!(value["protocol"]["name"], "smithers.nanocodex");
    assert_eq!(value["protocol"]["versions"], serde_json::json!([1]));
    assert_eq!(value["nanocodexVersion"], "0.3.0");
    assert_eq!(
        value["checkpoint"]["continuationModes"],
        serde_json::json!(["resume"])
    );
    assert_eq!(value["features"]["customEndpoints"], false);
    assert_eq!(value["features"]["subagents"], false);
}

#[test]
fn serve_emits_hello_then_one_process_failure_for_an_invalid_first_command() {
    let mut child = binary()
        .args(["serve", "--protocol-version", "1"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("serve command failed to start");
    child
        .stdin
        .as_mut()
        .unwrap()
        .write_all(b"{\"not\":\"a command\"}\n")
        .unwrap();
    drop(child.stdin.take());
    let output = child
        .wait_with_output()
        .expect("serve command did not exit");
    assert_eq!(output.status.code(), Some(2));
    assert!(output.stderr.is_empty());
    let records = output
        .stdout
        .split(|byte| *byte == b'\n')
        .filter(|line| !line.is_empty())
        .map(|line| serde_json::from_slice::<Value>(line).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(records.len(), 2);
    assert_eq!(records[0]["type"], "hello");
    assert_eq!(records[0]["seq"], 1);
    assert_eq!(records[1]["type"], "process.failed");
    assert_eq!(records[1]["seq"], 2);
    assert_eq!(records[1]["data"]["error"]["code"], "invalid_json");
}

#[test]
fn unsupported_protocol_version_exits_before_startup() {
    let output = binary()
        .args(["serve", "--protocol-version", "2"])
        .output()
        .expect("serve command failed to start");
    assert_eq!(output.status.code(), Some(2));
    assert!(output.stdout.is_empty());
    assert!(!output.stderr.is_empty());
}

#[test]
fn backend_failure_exits_while_the_parent_keeps_stdin_open() {
    let workspace = tempfile::tempdir().unwrap();
    let start = serde_json::json!({
        "protocol": "smithers.nanocodex",
        "version": 1,
        "type": "turn.start",
        "commandId": "start-missing-auth",
        "requestId": "request-missing-auth",
        "data": {
            "prompt": "do not reach the provider",
            "workspace": std::fs::canonicalize(workspace.path()).unwrap(),
            "auth": {
                "mode": "api-key-env",
                "environmentVariable": "SMITHERS_NANOCODEX_INTENTIONALLY_MISSING_KEY"
            },
            "transport": {"kind": "websocket"},
            "options": {},
            "continuation": null
        }
    });
    let mut child = binary()
        .args(["serve", "--protocol-version", "1"])
        .env_remove("SMITHERS_NANOCODEX_INTENTIONALLY_MISSING_KEY")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("serve command failed to start");
    let mut open_stdin = child.stdin.take().expect("child stdin was not piped");
    writeln!(open_stdin, "{}", serde_json::to_string(&start).unwrap()).unwrap();
    let mut stdout = BufReader::new(child.stdout.take().expect("child stdout was not piped"));
    let mut hello = String::new();
    stdout.read_line(&mut hello).expect("failed to read hello");
    assert_eq!(
        serde_json::from_str::<Value>(&hello).unwrap()["type"],
        "hello"
    );
    let mut terminal = String::new();
    stdout
        .read_line(&mut terminal)
        .expect("failed to read auth terminal");
    assert_eq!(
        serde_json::from_str::<Value>(&terminal).unwrap()["data"]["error"]["code"],
        "auth_unavailable"
    );

    let deadline = Instant::now() + Duration::from_secs(2);
    let status = loop {
        if let Some(status) = child.try_wait().expect("failed to poll child") {
            break status;
        }
        if Instant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            panic!("bridge did not exit after backend failure while stdin remained open");
        }
        thread::sleep(Duration::from_millis(10));
    };
    assert_eq!(status.code(), Some(3));
    drop(open_stdin);
}

#[cfg(unix)]
#[test]
fn sigterm_exits_while_the_parent_keeps_stdin_open() {
    let mut child = binary()
        .args(["serve", "--protocol-version", "1"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("serve command failed to start");
    let _open_stdin = child.stdin.take().expect("child stdin was not piped");
    let mut stdout = BufReader::new(child.stdout.take().expect("child stdout was not piped"));
    let mut hello = String::new();
    stdout.read_line(&mut hello).expect("failed to read hello");
    assert_eq!(
        serde_json::from_str::<Value>(&hello).unwrap()["type"],
        "hello"
    );

    // SAFETY: the PID belongs to the child spawned above and SIGTERM is a
    // process-directed signal supported on this Unix-only test path.
    assert_eq!(unsafe { libc::kill(child.id() as i32, libc::SIGTERM) }, 0);
    let mut terminal = String::new();
    stdout
        .read_line(&mut terminal)
        .expect("failed to read signal terminal");
    assert_eq!(
        serde_json::from_str::<Value>(&terminal).unwrap()["data"]["error"]["code"],
        "terminated_before_start"
    );

    let deadline = Instant::now() + Duration::from_secs(2);
    let status = loop {
        if let Some(status) = child.try_wait().expect("failed to poll child") {
            break status;
        }
        if Instant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            panic!("bridge did not exit while stdin remained open");
        }
        thread::sleep(Duration::from_millis(10));
    };
    assert_eq!(status.code(), Some(130));
}
