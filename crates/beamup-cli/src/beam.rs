use std::path::{Path, PathBuf};
use std::process::{ExitStatus, Stdio};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use anyhow::{Context, Result};
use serde::Deserialize;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::Command;
use tracing::{debug, info, warn};

/// Collected stderr lines from the remote agent, shared with the draining task.
#[derive(Clone, Default)]
pub struct AgentStderr(Arc<Mutex<Vec<String>>>);

impl AgentStderr {
    fn push(&self, line: String) {
        if let Ok(mut lines) = self.0.lock() {
            // Bound it; a chatty agent shouldn't grow this without limit.
            if lines.len() < 100 {
                lines.push(line);
            }
        }
    }

    /// Recent agent stderr, newline-joined, or None if it said nothing.
    pub fn contents(&self) -> Option<String> {
        let lines = self.0.lock().ok()?;
        if lines.is_empty() {
            return None;
        }
        Some(lines.join("\n"))
    }
}

static IDENTITY_FILE: OnceLock<Option<PathBuf>> = OnceLock::new();
static PROXY: OnceLock<Option<String>> = OnceLock::new();

/// Set the identity file and proxy for all tsh invocations.
pub fn set_identity_file(path: Option<PathBuf>, proxy: Option<String>) {
    IDENTITY_FILE.get_or_init(|| path);
    PROXY.get_or_init(|| proxy);
}

/// Build a tsh Command with identity/proxy args prepended if configured.
fn tsh_command() -> Command {
    let mut cmd = Command::new("tsh");
    if let Some(Some(path)) = IDENTITY_FILE.get() {
        cmd.arg("-i").arg(path);
    }
    if let Some(Some(proxy)) = PROXY.get() {
        cmd.arg("--proxy").arg(proxy);
    }
    cmd
}

/// Build a std::process::Command (sync) with identity/proxy args prepended if configured.
pub fn tsh_command_sync() -> std::process::Command {
    let mut cmd = std::process::Command::new("tsh");
    if let Some(Some(path)) = IDENTITY_FILE.get() {
        cmd.arg("-i").arg(path);
    }
    if let Some(Some(proxy)) = PROXY.get() {
        cmd.arg("--proxy").arg(proxy);
    }
    cmd
}

const EMBEDDED_AGENT_X86_64: &[u8] = include_bytes!(concat!(env!("OUT_DIR"), "/beamup-agent-x86_64"));
const EMBEDDED_AGENT_AARCH64: &[u8] = include_bytes!(concat!(env!("OUT_DIR"), "/beamup-agent-aarch64"));

const MAX_RETRIES: u32 = 3;
const INITIAL_BACKOFF: Duration = Duration::from_secs(1);

/// Architecture of the remote beam, as reported by `uname -m`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BeamArch {
    X86_64,
    Aarch64,
}

impl BeamArch {
    fn parse(uname_m: &str) -> Result<Self> {
        match uname_m.trim() {
            "x86_64" | "amd64" => Ok(BeamArch::X86_64),
            "aarch64" | "arm64" => Ok(BeamArch::Aarch64),
            other => anyhow::bail!("unsupported beam architecture: {other}"),
        }
    }

    fn as_str(&self) -> &'static str {
        match self {
            BeamArch::X86_64 => "x86_64",
            BeamArch::Aarch64 => "aarch64",
        }
    }

    fn target_triple(&self) -> &'static str {
        match self {
            BeamArch::X86_64 => "x86_64-unknown-linux-musl",
            BeamArch::Aarch64 => "aarch64-unknown-linux-musl",
        }
    }

    fn embedded(&self) -> &'static [u8] {
        match self {
            BeamArch::X86_64 => EMBEDDED_AGENT_X86_64,
            BeamArch::Aarch64 => EMBEDDED_AGENT_AARCH64,
        }
    }
}

/// Ask the beam what architecture it is, so we deploy an agent that can actually exec.
async fn detect_beam_arch(beam_id: &str) -> Result<BeamArch> {
    let output = tsh_command()
        .args(["beams", "exec", beam_id, "--", "uname", "-m"])
        .stdin(Stdio::null())
        .output()
        .await
        .context("failed to run uname on beam")?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        anyhow::bail!("failed to detect beam architecture: {}", stderr.trim());
    }

    let stdout = String::from_utf8_lossy(&output.stdout);
    // tsh may prepend its own chatter; the arch is the last non-empty line.
    let line = stdout
        .lines()
        .filter(|l| !l.trim().is_empty())
        .next_back()
        .unwrap_or("");
    let arch = BeamArch::parse(line)?;
    debug!("beam {beam_id} architecture: {}", arch.as_str());
    Ok(arch)
}

fn agent_binary_path(arch: BeamArch) -> Result<AgentBinary> {
    // Arch-specific env override, then the generic one (for development)
    let arch_var = format!("BEAMUP_AGENT_PATH_{}", arch.as_str().to_uppercase());
    for var in [arch_var.as_str(), "BEAMUP_AGENT_PATH"] {
        if let Ok(path) = std::env::var(var) {
            let p = PathBuf::from(path);
            if p.exists() {
                return Ok(AgentBinary::Path(p));
            }
        }
    }

    // Check workspace target directory (development)
    let triple = arch.target_triple();
    for profile in ["release", "debug"] {
        let p = PathBuf::from(format!("target/{triple}/{profile}/beamup-agent"));
        if p.exists() {
            return Ok(AgentBinary::Path(p));
        }
    }

    // Use embedded binary if available (non-empty)
    let embedded = arch.embedded();
    if !embedded.is_empty() {
        return Ok(AgentBinary::Embedded(embedded));
    }

    // Check next to our own binary (for installed/packaged deployments)
    if let Ok(exe) = std::env::current_exe() {
        let dir = exe.parent().unwrap_or(exe.as_ref());
        for name in [format!("beamup-agent-{}", arch.as_str()), "beamup-agent".to_string()] {
            let sibling = dir.join(name);
            if sibling.exists() {
                return Ok(AgentBinary::Path(sibling));
            }
        }
    }

    anyhow::bail!(
        "no beamup-agent binary for beam architecture {}. Build it with:\n  \
         cargo build --release --target {triple} -p beamup-agent\n\
         Or set {arch_var} to point to the binary.",
        arch.as_str()
    )
}

enum AgentBinary {
    Path(PathBuf),
    Embedded(&'static [u8]),
}

impl AgentBinary {
    fn to_path(&self, arch: BeamArch) -> Result<PathBuf> {
        match self {
            AgentBinary::Path(p) => Ok(p.clone()),
            AgentBinary::Embedded(data) => {
                // Arch in the name so staged agents for different beams can't collide.
                let tmp = std::env::temp_dir().join(format!("beamup-agent-{}", arch.as_str()));
                std::fs::write(&tmp, data)?;
                #[cfg(unix)]
                {
                    use std::os::unix::fs::PermissionsExt;
                    std::fs::set_permissions(&tmp, std::fs::Permissions::from_mode(0o755))?;
                }
                Ok(tmp)
            }
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct BeamInfo {
    #[serde(alias = "name", alias = "id")]
    pub id: String,
}

pub struct Beam;

impl Beam {
    pub async fn create() -> Result<BeamInfo> {
        let output = tsh_command()
            .args(["beams", "add", "--format=json"])
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .output()
            .await
            .context("failed to run tsh beams add")?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            anyhow::bail!("tsh beams add failed: {stderr}");
        }

        let info: BeamInfo = serde_json::from_slice(&output.stdout)
            .context("failed to parse tsh beams add output")?;
        info!("created beam: {}", info.id);
        Ok(info)
    }

    pub async fn destroy(beam_id: &str) -> Result<()> {
        let output = tsh_command()
            .args(["beams", "rm", beam_id])
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .output()
            .await
            .context("failed to run tsh beams rm")?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            anyhow::bail!("tsh beams rm failed: {stderr}");
        }

        info!("destroyed beam: {beam_id}");
        Ok(())
    }

    pub async fn list() -> Result<Vec<BeamInfo>> {
        let output = tsh_command()
            .args(["beams", "ls", "--format=json"])
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .output()
            .await
            .context("failed to run tsh beams ls")?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            anyhow::bail!("tsh beams ls failed: {stderr}");
        }

        let stdout = String::from_utf8_lossy(&output.stdout);
        if stdout.trim().is_empty() {
            return Ok(Vec::new());
        }

        let beams: Vec<BeamInfo> = serde_json::from_str(&stdout)
            .context("failed to parse tsh beams ls output")?;
        Ok(beams)
    }

    pub async fn deploy_agent(beam_id: &str, concurrency: usize) -> Result<()> {
        let arch = detect_beam_arch(beam_id).await?;
        info!("beam architecture: {}", arch.as_str());
        let agent = agent_binary_path(arch)?;
        let agent_path = agent.to_path(arch)?;
        crate::transfer::deploy_agent_chunked(beam_id, &agent_path, concurrency).await
    }

    pub fn spawn_agent(beam_id: &str, remote_dir: &str) -> Result<(tokio::process::Child, AgentStderr)> {
        let mut child = tsh_command()
            .args([
                "beams", "exec", beam_id, "--",
                "/tmp/beamup-agent", "--serve", "--watch-dir", remote_dir,
            ])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .context("failed to spawn agent via tsh beams exec")?;

        // Drain stderr rather than letting it fill an unread pipe: if the agent dies
        // at startup (wrong arch, missing binary) this is the only place that says why.
        let stderr_log = AgentStderr::default();
        if let Some(stderr) = child.stderr.take() {
            let sink = stderr_log.clone();
            tokio::spawn(async move {
                let mut lines = BufReader::new(stderr).lines();
                while let Ok(Some(line)) = lines.next_line().await {
                    if line.trim().is_empty() {
                        continue;
                    }
                    warn!("agent stderr: {line}");
                    sink.push(line);
                }
            });
        }

        debug!("agent process spawned for beam {beam_id}");
        Ok((child, stderr_log))
    }

    pub async fn exec_interactive(beam_id: &str, cmd: &[String]) -> Result<ExitStatus> {
        let mut args = vec!["beams", "exec", beam_id, "--"];
        let cmd_refs: Vec<&str> = cmd.iter().map(|s| s.as_str()).collect();
        args.extend(cmd_refs);

        let status = tsh_command()
            .args(&args)
            .stdin(Stdio::inherit())
            .stdout(Stdio::inherit())
            .stderr(Stdio::inherit())
            .status()
            .await
            .context("failed to exec in beam")?;

        Ok(status)
    }

    pub async fn console(beam_id: &str) -> Result<ExitStatus> {
        let status = tsh_command()
            .args(["beams", "console", beam_id])
            .stdin(Stdio::inherit())
            .stdout(Stdio::inherit())
            .stderr(Stdio::inherit())
            .status()
            .await
            .context("failed to open console on beam")?;

        Ok(status)
    }

    /// scp a local file to the beam (with retry)
    pub async fn scp_to_beam(beam_id: &str, local_path: &Path, remote_path: &str) -> Result<()> {
        let dest = format!("{beam_id}:{remote_path}");
        let mut last_err = None;

        for attempt in 0..=MAX_RETRIES {
            if attempt > 0 {
                let backoff = INITIAL_BACKOFF * 2u32.pow(attempt - 1);
                warn!("scp push retry {attempt}/{MAX_RETRIES} for {} (backoff {:?})", local_path.display(), backoff);
                tokio::time::sleep(backoff).await;
            }

            let output = tsh_command()
                .args(["beams", "scp", &local_path.to_string_lossy(), &dest])
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .output()
                .await
                .context("failed to run tsh beams scp (push)")?;

            if output.status.success() {
                return Ok(());
            }

            last_err = Some(String::from_utf8_lossy(&output.stderr).to_string());
        }

        anyhow::bail!("scp push failed for {} after {MAX_RETRIES} retries: {}", local_path.display(), last_err.unwrap_or_default())
    }

    /// scp a file from the beam to local (with retry)
    pub async fn scp_from_beam(beam_id: &str, remote_path: &str, local_path: &Path) -> Result<()> {
        if let Some(parent) = local_path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        let src = format!("{beam_id}:{remote_path}");
        let mut last_err = None;

        for attempt in 0..=MAX_RETRIES {
            if attempt > 0 {
                let backoff = INITIAL_BACKOFF * 2u32.pow(attempt - 1);
                warn!("scp pull retry {attempt}/{MAX_RETRIES} for {remote_path} (backoff {:?})", backoff);
                tokio::time::sleep(backoff).await;
            }

            let output = tsh_command()
                .args(["beams", "scp", &src, &local_path.to_string_lossy()])
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .output()
                .await
                .context("failed to run tsh beams scp (pull)")?;

            if output.status.success() {
                return Ok(());
            }

            last_err = Some(String::from_utf8_lossy(&output.stderr).to_string());
        }

        anyhow::bail!("scp pull failed for {remote_path} after {MAX_RETRIES} retries: {}", last_err.unwrap_or_default())
    }

    /// Run a shell command string in the beam (with retry)
    pub async fn exec_shell(beam_id: &str, shell_cmd: &str) -> Result<()> {
        let mut last_err = None;

        for attempt in 0..=MAX_RETRIES {
            if attempt > 0 {
                let backoff = INITIAL_BACKOFF * 2u32.pow(attempt - 1);
                warn!("exec_shell retry {attempt}/{MAX_RETRIES} (backoff {:?})", backoff);
                tokio::time::sleep(backoff).await;
            }

            let output = tsh_command()
                .args(["beams", "exec", beam_id, "--", shell_cmd])
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .output()
                .await
                .context("failed to exec shell in beam")?;

            if output.status.success() {
                return Ok(());
            }

            last_err = Some(String::from_utf8_lossy(&output.stderr).to_string());
        }

        anyhow::bail!("shell exec failed after {MAX_RETRIES} retries: {}", last_err.unwrap_or_default())
    }

    /// Run a non-interactive command in the beam (with retry)
    pub async fn exec_cmd(beam_id: &str, cmd: &[&str]) -> Result<()> {
        let mut args = vec!["beams", "exec", beam_id, "--"];
        args.extend(cmd);
        let mut last_err = None;

        for attempt in 0..=MAX_RETRIES {
            if attempt > 0 {
                let backoff = INITIAL_BACKOFF * 2u32.pow(attempt - 1);
                warn!("exec_cmd retry {attempt}/{MAX_RETRIES} (backoff {:?})", backoff);
                tokio::time::sleep(backoff).await;
            }

            let output = tsh_command()
                .args(&args)
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .output()
                .await
                .context("failed to exec in beam")?;

            if output.status.success() {
                return Ok(());
            }

            last_err = Some(String::from_utf8_lossy(&output.stderr).to_string());
        }

        anyhow::bail!("exec failed ({}) after {MAX_RETRIES} retries: {}", cmd.join(" "), last_err.unwrap_or_default())
    }

    /// Run a non-interactive command and capture stdout
    pub async fn exec_cmd_output(beam_id: &str, cmd: &[&str]) -> Result<String> {
        let mut args = vec!["beams", "exec", beam_id, "--"];
        args.extend(cmd);

        let output = tsh_command()
            .args(&args)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .output()
            .await
            .context("failed to exec in beam")?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            anyhow::bail!("exec failed ({}): {stderr}", cmd.join(" "));
        }

        Ok(String::from_utf8_lossy(&output.stdout).to_string())
    }
}
