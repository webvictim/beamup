use std::path::{Path, PathBuf};
use std::process::{ExitStatus, Stdio};
use std::sync::OnceLock;
use std::time::Duration;

use anyhow::{Context, Result};
use serde::Deserialize;
use tokio::process::Command;
use tracing::{debug, info, warn};

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

const EMBEDDED_AGENT: &[u8] = include_bytes!(concat!(env!("OUT_DIR"), "/beamup-agent-embedded"));

const MAX_RETRIES: u32 = 3;
const INITIAL_BACKOFF: Duration = Duration::from_secs(1);

fn agent_binary_path() -> Result<AgentBinary> {
    // Check env var first (for development override)
    if let Ok(path) = std::env::var("BEAMUP_AGENT_PATH") {
        let p = PathBuf::from(path);
        if p.exists() {
            return Ok(AgentBinary::Path(p));
        }
    }

    // Check workspace target directory (development)
    let candidates = [
        "target/aarch64-unknown-linux-musl/release/beamup-agent",
        "target/aarch64-unknown-linux-musl/debug/beamup-agent",
    ];
    for candidate in &candidates {
        let p = PathBuf::from(candidate);
        if p.exists() {
            return Ok(AgentBinary::Path(p));
        }
    }

    // Use embedded binary if available (non-empty)
    if !EMBEDDED_AGENT.is_empty() {
        return Ok(AgentBinary::Embedded(EMBEDDED_AGENT));
    }

    // Check next to our own binary (for installed/packaged deployments)
    if let Ok(exe) = std::env::current_exe() {
        let sibling = exe.parent().unwrap_or(exe.as_ref()).join("beamup-agent");
        if sibling.exists() {
            return Ok(AgentBinary::Path(sibling));
        }
    }

    anyhow::bail!(
        "beamup-agent binary not found. Build it with: \
         cross build --target aarch64-unknown-linux-musl -p beamup-agent\n\
         Or set BEAMUP_AGENT_PATH to point to the binary."
    )
}

enum AgentBinary {
    Path(PathBuf),
    Embedded(&'static [u8]),
}

impl AgentBinary {
    fn to_path(&self) -> Result<PathBuf> {
        match self {
            AgentBinary::Path(p) => Ok(p.clone()),
            AgentBinary::Embedded(data) => {
                let tmp = std::env::temp_dir().join("beamup-agent");
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
        let agent = agent_binary_path()?;
        let agent_path = agent.to_path()?;
        crate::transfer::deploy_agent_chunked(beam_id, &agent_path, concurrency).await
    }

    pub fn spawn_agent(beam_id: &str, remote_dir: &str) -> Result<tokio::process::Child> {
        let child = tsh_command()
            .args([
                "beams", "exec", beam_id, "--",
                "/tmp/beamup-agent", "--serve", "--watch-dir", remote_dir,
            ])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .context("failed to spawn agent via tsh beams exec")?;

        debug!("agent process spawned for beam {beam_id}");
        Ok(child)
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
