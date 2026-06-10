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
