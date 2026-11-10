pub mod down;
pub mod exec;
pub mod start;
pub mod status;
pub mod sync;

use beamup_protocol::messages::SyncDirection;
use clap::ValueEnum;

#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum CliSyncDirection {
    /// Sync only from local machine to beam
    #[value(name = "local-to-beam")]
    LocalToBeam,
    /// Sync only from beam to local machine
    #[value(name = "beam-to-local")]
    BeamToLocal,
    /// Sync in both directions
    #[value(name = "bidirectional")]
    Bidirectional,
}

impl From<CliSyncDirection> for SyncDirection {
    fn from(d: CliSyncDirection) -> Self {
        match d {
            CliSyncDirection::LocalToBeam => SyncDirection::LocalToBeam,
            CliSyncDirection::BeamToLocal => SyncDirection::BeamToLocal,
            CliSyncDirection::Bidirectional => SyncDirection::Bidirectional,
        }
    }
}

/// Describes a sync for the log, local on the left and beam on the right,
/// with an arrow for the way changes flow. `ongoing` is `None` for a
/// one-shot run, which only does the initial sync. When the two directions
/// differ, the arrow is the ongoing one and the initial one follows.
pub fn describe_sync(
    local: &std::path::Path,
    beam_id: &str,
    remote: &str,
    initial: SyncDirection,
    ongoing: Option<SyncDirection>,
) -> String {
    let main = ongoing.unwrap_or(initial);
    let mut out = format!("{} {} {}:{}", local.display(), main.arrow(), beam_id, remote);
    if main != initial {
        out.push_str(&format!(" (initial sync {})", initial.arrow()));
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    fn describe(initial: SyncDirection, ongoing: Option<SyncDirection>) -> String {
        describe_sync(Path::new("/Users/gus/beams"), "vapor-socket", "/home/beams", initial, ongoing)
    }

    #[test]
    fn arrow_follows_the_direction() {
        use SyncDirection::*;
        assert_eq!(describe(Bidirectional, Some(Bidirectional)), "/Users/gus/beams ↔ vapor-socket:/home/beams");
        assert_eq!(describe(LocalToBeam, Some(LocalToBeam)), "/Users/gus/beams → vapor-socket:/home/beams");
        assert_eq!(describe(BeamToLocal, Some(BeamToLocal)), "/Users/gus/beams ← vapor-socket:/home/beams");
    }

    #[test]
    fn differing_initial_direction_is_noted() {
        use SyncDirection::*;
        assert_eq!(
            describe(Bidirectional, Some(BeamToLocal)),
            "/Users/gus/beams ← vapor-socket:/home/beams (initial sync ↔)"
        );
    }

    #[test]
    fn oneshot_shows_only_the_initial_direction() {
        use SyncDirection::*;
        assert_eq!(describe(LocalToBeam, None), "/Users/gus/beams → vapor-socket:/home/beams");
    }
}
