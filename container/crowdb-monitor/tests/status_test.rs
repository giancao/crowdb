// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::Duration;

use crowdb_monitor::{MonitorPhase, MonitorStatus, ServiceStatus, StatusStore};
use uuid::Uuid;

struct TestRunRoot(PathBuf);

impl TestRunRoot {
    fn new() -> Self {
        let root = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../.crowdb-runtime/ephemeral")
            .join(format!("monitor-status-{}", Uuid::new_v4()));
        fs::create_dir_all(&root).unwrap();
        Self(root.canonicalize().unwrap())
    }
}

impl Drop for TestRunRoot {
    fn drop(&mut self) {
        if !std::thread::panicking() {
            fs::remove_dir_all(&self.0).unwrap();
        }
    }
}

fn command(name: &str, root: &TestRunRoot) -> bool {
    Command::new(env!("CARGO_BIN_EXE_crowdb-monitor"))
        .args([name, "--run-root"])
        .arg(&root.0)
        .output()
        .unwrap()
        .status
        .success()
}

#[test]
fn readiness_requires_fresh_ready_snapshot() {
    let root = TestRunRoot::new();
    assert!(!command("liveness", &root));
    assert!(!root.0.join("status").exists());
    let store = StatusStore::new(&root.0).unwrap();
    let mut status = MonitorStatus::new(Uuid::new_v4(), MonitorPhase::Initializing);
    store.publish(&mut status).unwrap();
    assert!(!command("liveness", &root));
    assert!(!command("readiness", &root));
    status.phase = MonitorPhase::Ready;
    status.services.insert(
        "kv".into(),
        ServiceStatus {
            operation_id: None,
            pid: Some(123),
            generation: 1,
            healthy: true,
            restart_attempts: 0,
        },
    );
    store.publish(&mut status).unwrap();
    assert_eq!(status.revision, 2);
    assert!(command("readiness", &root));
    let path = root.0.join("status/monitor.json");
    let mut stale: serde_json::Value = serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
    stale["updated_at_ms"] = 0.into();
    fs::write(&path, serde_json::to_vec(&stale).unwrap()).unwrap();
    assert!(store.read(Duration::from_secs(10)).is_err());
    status.phase = MonitorPhase::Restarting;
    store.publish(&mut status).unwrap();
    assert!(!command("readiness", &root));
}

fn readiness(run_root: &TestRunRoot, data_root: &Path) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_crowdb-monitor"))
        .args(["readiness", "--run-root"])
        .arg(&run_root.0)
        .arg("--data-root")
        .arg(data_root)
        .output()
        .unwrap()
}

fn record_initialization(data_root: &Path, value: &serde_json::Value, mode: u32) {
    use std::os::unix::fs::PermissionsExt;
    let path = data_root.join("initialization-status.json");
    fs::write(&path, serde_json::to_vec(value).unwrap()).unwrap();
    fs::set_permissions(&path, fs::Permissions::from_mode(mode)).unwrap();
}

#[test]
fn readiness_fails_after_cluster_initialization_failed() {
    let root = TestRunRoot::new();
    let data_root = root.0.join("data");
    fs::create_dir(&data_root).unwrap();
    let store = StatusStore::new(&root.0).unwrap();
    let mut status = MonitorStatus::new(Uuid::new_v4(), MonitorPhase::Ready);
    status.services.insert(
        "kv".into(),
        ServiceStatus {
            operation_id: None,
            pid: Some(123),
            generation: 1,
            healthy: true,
            restart_attempts: 0,
        },
    );
    store.publish(&mut status).unwrap();
    // A node which has not been asked to initialize is ready once its services are.
    assert!(readiness(&root, &data_root).status.success());
    for phase in ["preparing", "active"] {
        record_initialization(
            &data_root,
            &serde_json::json!({"phase": phase, "error": null}),
            0o600,
        );
        assert!(readiness(&root, &data_root).status.success(), "{phase}");
    }
    record_initialization(
        &data_root,
        &serde_json::json!({"phase": "failed", "error": "store open failed: NotSupported"}),
        0o600,
    );
    let failed = readiness(&root, &data_root);
    assert!(!failed.status.success());
    let error = String::from_utf8(failed.stderr).unwrap();
    assert!(
        error.contains("cluster initialization failed: store open failed: NotSupported"),
        "{error}"
    );
    // An unreadable record cannot prove that initialization did not fail.
    record_initialization(&data_root, &serde_json::json!({"phase": "active"}), 0o644);
    assert!(!readiness(&root, &data_root).status.success());
}

#[test]
fn corrupt_or_symlinked_status_fails_closed() {
    let root = TestRunRoot::new();
    let store = StatusStore::new(&root.0).unwrap();
    let mut status = MonitorStatus::new(Uuid::new_v4(), MonitorPhase::Ready);
    store.publish(&mut status).unwrap();
    fs::write(root.0.join("status/monitor.json"), b"not json").unwrap();
    assert!(!command("readiness", &root));
    fs::remove_file(root.0.join("status/monitor.json")).unwrap();
    std::os::unix::fs::symlink("/etc/passwd", root.0.join("status/monitor.json")).unwrap();
    assert!(!command("readiness", &root));
}
