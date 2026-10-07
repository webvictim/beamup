use std::path::{Path, PathBuf};
use std::process::{ExitStatus, Stdio};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use anyhow::{Context, Result};
use beamup_protocol::hash::hash_content;
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

/// What one round trip to the beam tells us before deploying: its architecture, whether
/// beam-init's `beamctl` is there to host a persistent agent, which of our agent builds
/// are already uploaded, and the state of this sync's agent service.
pub struct BeamProbe {
    pub arch: BeamArch,
    pub has_beamctl: bool,
    present_agents: Vec<String>,
    service: Option<ServiceInfo>,
}

/// The subset of `beamctl --json show` we care about.
#[derive(Debug, Deserialize)]
struct ServiceInfo {
    cmd: String,
    #[serde(default)]
    args: Vec<String>,
    status: serde_json::Value,
}

impl ServiceInfo {
    fn is_running(&self) -> bool {
        self.status.get("Running").is_some()
    }
}

impl BeamProbe {
    /// Parse the probe script's `key=value` lines, ignoring anything else tsh prints.
    fn parse(output: &str) -> Result<Self> {
        let mut arch = None;
        let mut has_beamctl = false;
        let mut present_agents = Vec::new();
        let mut service = None;
        for line in output.lines() {
            let Some((key, value)) = line.trim().split_once('=') else {
                continue;
            };
            match key {
                "arch" => arch = Some(BeamArch::parse(value)?),
                "beamctl" => has_beamctl = true,
                "agent" => present_agents.push(value.to_string()),
                // Empty when the service doesn't exist.
                "service" if !value.is_empty() => service = serde_json::from_str(value).ok(),
                _ => {}
            }
        }
        let arch = arch.ok_or_else(|| anyhow::anyhow!("beam probe did not report an architecture:\n{output}"))?;
        Ok(Self { arch, has_beamctl, present_agents, service })
    }
}

async fn probe_beam(beam_id: &str, remote_dir: &str) -> Result<BeamProbe> {
    // We can't know the arch before asking, so check for every agent we could deploy.
    let candidates: Vec<String> = [BeamArch::X86_64, BeamArch::Aarch64]
        .into_iter()
        .filter_map(|arch| agent_binary_path(arch).ok()?.remote_path().ok())
        .collect();
    let service = agent_service_name(remote_dir);
    let script = format!(
        "echo arch=$(uname -m); \
         if command -v beamctl >/dev/null 2>&1; then \
           echo beamctl=1; \
           echo service=$(beamctl --json show {service} 2>/dev/null | tr -d '\\n'); \
         fi; \
         for f in {}; do [ -x \"$f\" ] && echo agent=$f; done; true",
        candidates.join(" ")
    );
    let output = Beam::exec_shell_output(beam_id, &script).await?;
    let probe = BeamProbe::parse(&output)?;
    debug!(
        "beam {beam_id}: arch {}, beamctl {}, agents present {:?}, service {:?}",
        probe.arch.as_str(), probe.has_beamctl, probe.present_agents, probe.service
    );
    Ok(probe)
}

/// The deployed agent's path on the beam, for helpers that run it directly
/// (chunked transfers). Set once per process by `Beam::deploy_agent`.
static AGENT_REMOTE_PATH: OnceLock<String> = OnceLock::new();

pub fn agent_remote_path() -> &'static str {
    AGENT_REMOTE_PATH.get().map(String::as_str).unwrap_or("/tmp/beamup-agent")
}

static PERSISTENT_AGENT: OnceLock<bool> = OnceLock::new();

/// Whether to run the agent as a persistent beamctl service when the beam supports it.
pub fn set_persistent_agent(enabled: bool) {
    PERSISTENT_AGENT.get_or_init(|| enabled);
}

/// beamctl service name for the agent syncing `remote_dir`: one agent per directory.
pub fn agent_service_name(remote_dir: &str) -> String {
    format!("beamup-{:016x}", hash_content(remote_dir.as_bytes()))
}

fn agent_socket_path(remote_dir: &str) -> String {
    format!("/tmp/{}.sock", agent_service_name(remote_dir))
}

/// How the CLI reaches the agent on the beam.
pub enum AgentMode {
    /// The agent serves this one session over the `tsh beams exec` pipe and exits with it.
    Stdio,
    /// The agent runs as a beamctl service; each session relays to it over a socket.
    Service { name: String, socket: String },
}

impl AgentMode {
    pub fn service_name(&self) -> Option<&str> {
        match self {
            AgentMode::Stdio => None,
            AgentMode::Service { name, .. } => Some(name),
        }
    }
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
    /// Where this exact build lives on the beam. Named by content, so an agent that's
    /// already there is never re-uploaded and different builds never overwrite each other.
    fn remote_path(&self) -> Result<String> {
        let hash = match self {
            AgentBinary::Path(p) => hash_content(&std::fs::read(p)?),
            AgentBinary::Embedded(data) => hash_content(data),
        };
        Ok(format!("/tmp/beamup-agent-{hash:016x}"))
    }

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

/// (Re)start the agent service. beamctl refuses to start a name that exists in any
/// state, even exited, so always prune first.
async fn start_agent_service(beam_id: &str, name: &str, agent: &str, args: &[&str]) -> Result<()> {
    info!("starting agent service {name}");
    let args: Vec<String> = args.iter().map(|a| shell_quote(a)).collect();
    let script = format!(
        "beamctl stop --prune {name} >/dev/null 2>&1; \
         beamctl start --name {name} --labels app=beamup -- {agent} {}",
        args.join(" ")
    );
    Beam::exec_shell(beam_id, &script).await
}

/// Quote a string for the remote shell that `tsh beams exec` runs commands through.
pub fn shell_quote(s: &str) -> String {
    format!("'{}'", s.replace('\'', r"'\''"))
}

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

    /// Make sure the right agent is on the beam and, where beamctl allows, running as a
    /// persistent service for `remote_dir`. Returns how to reach it.
    pub async fn deploy_agent(beam_id: &str, remote_dir: &str) -> Result<AgentMode> {
        let probe = probe_beam(beam_id, remote_dir).await?;
        let arch = probe.arch;
        info!("beam architecture: {}", arch.as_str());
        let agent = agent_binary_path(arch)?;
        let remote_path = agent.remote_path()?;

        if probe.present_agents.contains(&remote_path) {
            info!("agent already on beam");
        } else {
            let agent_path = agent.to_path(arch)?;
            crate::transfer::upload_agent(beam_id, &agent_path, &remote_path).await?;
        }
        let _ = AGENT_REMOTE_PATH.set(remote_path.clone());

        let persistent = *PERSISTENT_AGENT.get().unwrap_or(&true);
        if !persistent || !probe.has_beamctl {
            return Ok(AgentMode::Stdio);
        }

        let name = agent_service_name(remote_dir);
        let socket = agent_socket_path(remote_dir);
        let args = ["--serve", "--listen", &socket, "--watch-dir", remote_dir];
        let reusable = probe.service.as_ref().is_some_and(|svc| {
            svc.is_running() && svc.cmd == remote_path && svc.args.iter().map(String::as_str).eq(args)
        });
        if reusable {
            info!("reusing running agent service {name}");
        } else if let Err(e) = start_agent_service(beam_id, &name, &remote_path, &args).await {
            warn!("could not start agent service, falling back to a per-session agent: {e:#}");
            return Ok(AgentMode::Stdio);
        }
        Ok(AgentMode::Service { name, socket })
    }

    pub fn spawn_agent(beam_id: &str, remote_dir: &str, mode: &AgentMode) -> Result<(tokio::process::Child, AgentStderr)> {
        let agent = agent_remote_path();
        let agent_args = match mode {
            AgentMode::Stdio => vec![agent, "--serve", "--watch-dir", remote_dir],
            AgentMode::Service { socket, .. } => vec![agent, "--connect", socket.as_str()],
        };
        let mut child = tsh_command()
            .args(["beams", "exec", beam_id, "--"])
            .args(agent_args)
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

    /// Run a shell command string in the beam and capture stdout (with retry)
    pub async fn exec_shell_output(beam_id: &str, shell_cmd: &str) -> Result<String> {
        let mut last_err = None;

        for attempt in 0..=MAX_RETRIES {
            if attempt > 0 {
                let backoff = INITIAL_BACKOFF * 2u32.pow(attempt - 1);
                warn!("exec_shell_output retry {attempt}/{MAX_RETRIES} (backoff {:?})", backoff);
                tokio::time::sleep(backoff).await;
            }

            let output = tsh_command()
                .args(["beams", "exec", beam_id, "--", shell_cmd])
                .stdin(Stdio::null())
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .output()
                .await
                .context("failed to exec shell in beam")?;

            if output.status.success() {
                return Ok(String::from_utf8_lossy(&output.stdout).to_string());
            }

            last_err = Some(String::from_utf8_lossy(&output.stderr).to_string());
        }

        anyhow::bail!("shell exec failed after {MAX_RETRIES} retries: {}", last_err.unwrap_or_default())
    }

    /// Last lines of the agent service's output, for diagnosing a failed handshake.
    pub async fn agent_service_logs(beam_id: &str, name: &str) -> Option<String> {
        let logs = Self::exec_cmd_output(beam_id, &["beamctl", "logs", name]).await.ok()?;
        let logs = logs.trim();
        (!logs.is_empty()).then(|| logs.to_string())
    }

    /// State of the agent service: "running", "stopped", "exited", "failed" or "absent".
    pub async fn agent_service_state(beam_id: &str, name: &str) -> Result<&'static str> {
        let script = format!("beamctl --json show {name} 2>/dev/null || true");
        let output = Self::exec_shell_output(beam_id, &script).await?;
        // Nothing on stdout means no such service.
        let Some(start) = output.find('{') else {
            return Ok("absent");
        };
        let info: ServiceInfo =
            serde_json::from_str(&output[start..]).context("failed to parse beamctl show output")?;
        Ok(match &info.status {
            s if s.get("Running").is_some() => "running",
            s if s.get("Exited").is_some() => "exited",
            s if s.as_str() == Some("Stopped") => "stopped",
            _ => "failed",
        })
    }

    /// Stop and remove beamup's agent services on the beam, if it has beamctl at all.
    pub async fn stop_agent_services(beam_id: &str) -> Result<()> {
        Self::exec_shell(
            beam_id,
            "if command -v beamctl >/dev/null 2>&1; then \
               beamctl stop --prune --selector app=beamup >/dev/null 2>&1; \
             fi; true",
        )
        .await
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn probe_parses_beam_with_beamctl_and_running_service() {
        let output = "some tsh chatter\n\
            arch=aarch64\n\
            beamctl=1\n\
            service={  \"cmd\": \"/tmp/beamup-agent-ab\",  \"args\": [\"--serve\"],  \"status\": {    \"Running\": {      \"main_pid\": 117,      \"pty\": null    }  }}\n\
            agent=/tmp/beamup-agent-ab\n";
        let probe = BeamProbe::parse(output).unwrap();
        assert_eq!(probe.arch, BeamArch::Aarch64);
        assert!(probe.has_beamctl);
        assert_eq!(probe.present_agents, vec!["/tmp/beamup-agent-ab"]);
        let svc = probe.service.unwrap();
        assert!(svc.is_running());
        assert_eq!(svc.cmd, "/tmp/beamup-agent-ab");
        assert_eq!(svc.args, vec!["--serve"]);
    }

    #[test]
    fn probe_parses_beam_without_beamctl_or_service() {
        let probe = BeamProbe::parse("arch=x86_64\n").unwrap();
        assert_eq!(probe.arch, BeamArch::X86_64);
        assert!(!probe.has_beamctl);
        assert!(probe.present_agents.is_empty());
        assert!(probe.service.is_none());

        // beamctl present, but no service by that name yet.
        let probe = BeamProbe::parse("arch=aarch64\nbeamctl=1\nservice=\n").unwrap();
        assert!(probe.has_beamctl);
        assert!(probe.service.is_none());
    }

    #[test]
    fn probe_reads_stopped_service_as_not_running() {
        let probe = BeamProbe::parse(
            "arch=aarch64\nbeamctl=1\nservice={\"cmd\":\"/tmp/a\",\"args\":[],\"status\":{\"Exited\":768}}\n",
        )
        .unwrap();
        assert!(!probe.service.unwrap().is_running());
    }

    #[test]
    fn probe_without_arch_is_an_error() {
        assert!(BeamProbe::parse("beamctl=1\n").is_err());
    }

    #[test]
    fn shell_quote_survives_single_quotes() {
        assert_eq!(shell_quote("/home/beams/sync"), "'/home/beams/sync'");
        assert_eq!(shell_quote("it's"), r"'it'\''s'");
    }

    #[test]
    fn service_name_is_stable_and_per_directory() {
        let a = agent_service_name("/home/beams/sync");
        assert_eq!(a, agent_service_name("/home/beams/sync"));
        assert_ne!(a, agent_service_name("/home/beams/other"));
        assert!(a.starts_with("beamup-"));
    }
}
