use anyhow::Result;
use clap::Args;

use crate::beam::Beam;
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
            // Only persistent agents have state worth asking the beam about.
            let agent = match &session.agent_service {
                Some(name) => Beam::agent_service_state(&session.beam_id, name).await.ok(),
                None => None,
            };
            if args.json {
                let mut json = serde_json::json!({
                    "beam_id": session.beam_id,
                    "local_dir": session.local_dir.display().to_string(),
                    "remote_dir": session.remote_dir,
                    "status": "active",
                });
                if let Some(state) = agent {
                    json["agent"] = serde_json::json!(state);
                }
                println!("{}", serde_json::to_string_pretty(&json)?);
            } else {
                eprintln!("Beam:       {}", session.beam_id);
                eprintln!("Local:      {}", session.local_dir.display());
                eprintln!("Remote:     {}", session.remote_dir);
                eprintln!("Status:     active");
                if let (Some(name), Some(state)) = (&session.agent_service, agent) {
                    eprintln!("Agent:      {name} ({state})");
                }
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
