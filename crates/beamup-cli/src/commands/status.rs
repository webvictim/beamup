use anyhow::Result;
use clap::Args;

use crate::config::Session;

#[derive(Args)]
pub struct StatusArgs {
    /// Output as JSON
    #[arg(long)]
    pub json: bool,
}

pub async fn run(args: StatusArgs) -> Result<()> {
    let session = Session::load()?;

    match session {
        Some(session) => {
            if args.json {
                let json = serde_json::json!({
                    "beam_id": session.beam_id,
                    "local_dir": session.local_dir.display().to_string(),
                    "remote_dir": session.remote_dir,
                    "status": "active",
                });
                println!("{}", serde_json::to_string_pretty(&json)?);
            } else {
                eprintln!("Beam:       {}", session.beam_id);
                eprintln!("Local:      {}", session.local_dir.display());
                eprintln!("Remote:     {}", session.remote_dir);
                eprintln!("Status:     active");
            }
        }
        None => {
            if args.json {
                println!(r#"{{"status": "inactive"}}"#);
            } else {
                eprintln!("No active beamup session.");
            }
        }
    }

    Ok(())
}
