use anyhow::Result;
use clap::Args;
use tracing::info;

use crate::beam::Beam;
use crate::config::Session;

#[derive(Args)]
pub struct DownArgs {
    /// Beam to shut down
    #[arg(short, long)]
    pub beam: Option<String>,

    /// Don't destroy the beam, just stop syncing
    #[arg(long)]
    pub keep_beam: bool,

    /// Don't wait for pending syncs
    #[arg(long)]
    pub force: bool,
}

pub async fn run(args: DownArgs) -> Result<()> {
    let beam_id = if let Some(id) = args.beam {
        id
    } else {
        let session = Session::load()?;
        match session {
            Some(s) => s.beam_id,
            None => anyhow::bail!("no active session — specify --beam"),
        }
    };

    if !args.keep_beam {
        eprintln!("Destroying beam: {beam_id}...");
        Beam::destroy(&beam_id).await?;
        eprintln!("Beam destroyed.");
    } else {
        eprintln!("Stopped syncing (beam {beam_id} kept alive).");
    }

    Session::remove()?;
    info!("session cleaned up");
    Ok(())
}
