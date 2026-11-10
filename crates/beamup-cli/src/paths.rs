//! Tilde expansion for the `--local-path` / `--remote-path` arguments.
//!
//! Neither path is ever seen by a shell: the local one arrives from clap as a
//! `PathBuf`, and the remote one is handed straight to `beamup-agent
//! --watch-dir` through `tsh beams exec`. A `~` that survives shell quoting
//! (or `--local-path=~/x`, which no shell expands) therefore has to be
//! expanded here or it is taken literally — which previously meant a confusing
//! `No such file or directory` locally, and a directory actually named `~` on
//! the beam.

use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use directories::UserDirs;

/// Home directory of the `beams` user on a beam VM.
const BEAM_HOME: &str = "/home/beams";

/// Expand a leading `~` in a local path against this machine's home directory.
/// Paths without a tilde are returned unchanged, relative ones included —
/// `canonicalize` still resolves those against the current directory.
pub fn expand_local(path: &Path) -> Result<PathBuf> {
    // A non-UTF-8 path was written literally; leave it alone.
    let Some(s) = path.to_str() else {
        return Ok(path.to_path_buf());
    };

    let Some(rest) = tilde_rest(s)? else {
        return Ok(path.to_path_buf());
    };

    let home = UserDirs::new()
        .context("cannot determine the home directory to expand `~` in --local-path")?
        .home_dir()
        .to_path_buf();

    Ok(if rest.is_empty() { home } else { home.join(rest) })
}

/// Resolve a remote path against the beam's home directory: `~` and `~/...`
/// expand to it, and a relative path is taken to be under it. Without this the
/// agent would resolve a relative path against whatever its working directory
/// happens to be.
pub fn expand_remote(path: &str) -> Result<String> {
    if let Some(rest) = tilde_rest(path)? {
        return Ok(join_beam_home(rest));
    }

    if path.starts_with('/') {
        return Ok(path.to_string());
    }

    Ok(join_beam_home(path))
}

/// Strip a leading `~` and return the remainder (empty for a bare `~`), or
/// `None` when there is no tilde to expand. `~user` is rejected rather than
/// left literal: it cannot be resolved on either side — the local side would
/// need a passwd lookup, and the remote side only ever runs as `beams` — and a
/// literal `~user` path silently syncs to the wrong place.
fn tilde_rest(path: &str) -> Result<Option<&str>> {
    let Some(rest) = path.strip_prefix('~') else {
        return Ok(None);
    };

    if rest.is_empty() {
        return Ok(Some(""));
    }

    match rest.strip_prefix('/') {
        Some(rest) => Ok(Some(rest)),
        None => anyhow::bail!("cannot expand `~{rest}`: give an absolute path instead"),
    }
}

fn join_beam_home(rest: &str) -> String {
    let rest = rest.trim_start_matches('/');
    if rest.is_empty() {
        BEAM_HOME.to_string()
    } else {
        format!("{BEAM_HOME}/{rest}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn remote_tilde_expands_to_beam_home() {
        assert_eq!(expand_remote("~/git").unwrap(), "/home/beams/git");
        assert_eq!(expand_remote("~").unwrap(), "/home/beams");
        assert_eq!(expand_remote("~/").unwrap(), "/home/beams");
    }

    #[test]
    fn remote_relative_is_under_beam_home() {
        assert_eq!(expand_remote("sync").unwrap(), "/home/beams/sync");
        assert_eq!(expand_remote("git/beamup").unwrap(), "/home/beams/git/beamup");
    }

    #[test]
    fn remote_absolute_is_untouched() {
        assert_eq!(expand_remote("/home/beams/sync").unwrap(), "/home/beams/sync");
        assert_eq!(expand_remote("/tmp/x").unwrap(), "/tmp/x");
    }

    #[test]
    fn named_user_tilde_is_rejected() {
        let err = expand_remote("~alice/git").unwrap_err().to_string();
        assert!(err.contains("~alice/git"), "unexpected error: {err}");
        assert!(expand_local(Path::new("~alice/git")).is_err());
    }

    #[test]
    fn local_tilde_expands_to_home() {
        let home = UserDirs::new().unwrap().home_dir().to_path_buf();
        assert_eq!(expand_local(Path::new("~")).unwrap(), home);
        assert_eq!(expand_local(Path::new("~/git/beamup")).unwrap(), home.join("git/beamup"));
    }

    #[test]
    fn local_without_tilde_is_untouched() {
        assert_eq!(expand_local(Path::new("/Users/gus/git")).unwrap(), Path::new("/Users/gus/git"));
        assert_eq!(expand_local(Path::new("../sibling")).unwrap(), Path::new("../sibling"));
    }
}
