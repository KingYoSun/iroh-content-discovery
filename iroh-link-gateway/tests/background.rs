//! Installed gateway lifecycle checks without registering a login service.
use std::{
    fs,
    net::TcpListener,
    path::{Path, PathBuf},
    process::Command,
    sync::mpsc,
    time::Duration,
};

const HELPER: &str = env!("CARGO_BIN_EXE_iroh-link-gateway-background");
struct StopOnDrop(PathBuf);
impl Drop for StopOnDrop {
    fn drop(&mut self) {
        let _ = helper(&self.0, &["stop"]);
    }
}
fn helper(state: &Path, args: &[&str]) -> std::process::Output {
    let mut command = Command::new(HELPER);
    command.arg("--state-dir").arg(state).args(args);
    let (sender, receiver) = mpsc::channel();
    std::thread::spawn(move || {
        // Keep capturing pipes: returning before the daemon exits is part of
        // the launcher's contract, including on Windows.
        let _ = sender.send(command.output());
    });
    receiver
        .recv_timeout(Duration::from_secs(45))
        .unwrap_or_else(|error| {
            panic!(
                "helper {args:?} did not finish: {error}\nlauncher: {}\ngateway: {}",
                fs::read_to_string(state.join("launcher.log")).unwrap_or_default(),
                fs::read_to_string(state.join("gateway.log")).unwrap_or_default(),
            )
        })
        .unwrap()
}
fn require_success(state: &Path, args: &[&str]) {
    let output = helper(state, args);
    assert!(
        output.status.success(),
        "{:?}: {}\n{}",
        args,
        String::from_utf8_lossy(&output.stderr),
        fs::read_to_string(state.join("launcher.log")).unwrap_or_default()
    );
}
#[test]
fn start_stop_restart_preserve_arguments_and_ignore_stale_files() {
    let temporary = tempfile::tempdir().unwrap();
    let state = temporary.path().join("state with spaces");
    fs::create_dir(&state).unwrap();
    let _stop = StopOnDrop(state.clone());
    let arguments = r#"["--listen","127.0.0.1:0","--index-server","127.0.0.1:9"]"#;
    fs::write(state.join("arguments.json"), arguments).unwrap();
    fs::write(state.join("gateway.lock"), b"stale").unwrap();
    fs::write(state.join("ready"), b"stale").unwrap();
    fs::write(state.join("stop"), b"stale").unwrap();
    assert!(!helper(&state, &["status"]).status.success());
    require_success(&state, &[]);
    require_success(&state, &["status"]);
    require_success(&state, &[]);
    require_success(&state, &["stop"]);
    assert!(!helper(&state, &["status"]).status.success());
    assert!(!state.join("ready").exists());
    require_success(&state, &["stop"]);
    require_success(&state, &[]);
    require_success(&state, &["stop"]);
    assert_eq!(
        fs::read_to_string(state.join("arguments.json")).unwrap(),
        arguments
    );
}
#[test]
fn occupied_port_fails_without_stopping_its_owner() {
    let temporary = tempfile::tempdir().unwrap();
    let state = temporary.path();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    fs::write(
        state.join("arguments.json"),
        format!(r#"["--listen","{addr}","--index-server","127.0.0.1:9"]"#),
    )
    .unwrap();
    let _stop = StopOnDrop(state.into());
    assert!(!helper(state, &[]).status.success());
    assert!(!helper(state, &["status"]).status.success());
    assert!(!state.join("ready").exists());
    assert!(std::net::TcpStream::connect(addr).is_ok());
}
