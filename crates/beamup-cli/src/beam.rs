use std::path::{Path, PathBuf};
use std::process::{ExitStatus, Stdio};

use anyhow::{Context, Result};
use serde::Deserialize;
use tokio::process::Command;
use tracing::{debug, info};

const EMBEDDED_AGENT: &[u8] = include_bytes!(concat!(env!("OUT_DIR"), "/beamup-agent-embedded"));

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
        let output = Command::new("tsh")
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
        let output = Command::new("tsh")
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
        let output = Command::new("tsh")
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
        let child = Command::new("tsh")
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

        let status = Command::new("tsh")
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
        let status = Command::new("tsh")
            .args(["beams", "console", beam_id])
            .stdin(Stdio::inherit())
            .stdout(Stdio::inherit())
            .stderr(Stdio::inherit())
            .status()
            .await
            .context("failed to open console on beam")?;

        Ok(status)
    }

    /// scp a local file to the beam
    pub async fn scp_to_beam(beam_id: &str, local_path: &Path, remote_path: &str) -> Result<()> {
        let dest = format!("{beam_id}:{remote_path}");
        let output = Command::new("tsh")
            .args(["beams", "scp", &local_path.to_string_lossy(), &dest])
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .output()
            .await
            .context("failed to run tsh beams scp (push)")?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            anyhow::bail!("scp push failed for {}: {stderr}", local_path.display());
        }

        Ok(())
    }

    /// scp a file from the beam to local
    pub async fn scp_from_beam(beam_id: &str, remote_path: &str, local_path: &Path) -> Result<()> {
        if let Some(parent) = local_path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        let src = format!("{beam_id}:{remote_path}");
        let output = Command::new("tsh")
            .args(["beams", "scp", &src, &local_path.to_string_lossy()])
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .output()
            .await
            .context("failed to run tsh beams scp (pull)")?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            anyhow::bail!("scp pull failed for {remote_path}: {stderr}");
        }

        Ok(())
    }

    /// Run a shell command string in the beam (handles redirects, pipes, etc.)
    /// tsh beams exec already wraps in `bash -c`, so we pass the command directly.
    pub async fn exec_shell(beam_id: &str, shell_cmd: &str) -> Result<()> {
        let output = Command::new("tsh")
            .args(["beams", "exec", beam_id, "--", shell_cmd])
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .output()
            .await
            .context("failed to exec shell in beam")?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            anyhow::bail!("shell exec failed: {stderr}");
        }

        Ok(())
    }

    /// Run a non-interactive command in the beam (no output captured)
    pub async fn exec_cmd(beam_id: &str, cmd: &[&str]) -> Result<()> {
        let mut args = vec!["beams", "exec", beam_id, "--"];
        args.extend(cmd);

        let output = Command::new("tsh")
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

        Ok(())
    }

    /// Run a non-interactive command and capture stdout
    pub async fn exec_cmd_output(beam_id: &str, cmd: &[&str]) -> Result<String> {
        let mut args = vec!["beams", "exec", beam_id, "--"];
        args.extend(cmd);

        let output = Command::new("tsh")
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
