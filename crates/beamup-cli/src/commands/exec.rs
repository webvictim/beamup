use anyhow::Result;
use clap::Args;

use crate::beam::Beam;
use crate::config::Session;

#[derive(Args)]
pub struct ExecArgs {
    /// Beam to exec in
    #[arg(short, long)]
    pub beam: Option<String>,

    /// Command to run
    #[arg(trailing_var_arg = true, required = true)]
    pub cmd: Vec<String>,
}

pub async fn run(args: ExecArgs) -> Result<()> {
    let beam_id = if let Some(id) = args.beam {
        id
    } else {
        let session = Session::load()?;
        match session {
            Some(s) => s.beam_id,
            None => anyhow::bail!("no active session — specify --beam"),
        }
    };

    let status = Beam::exec_interactive(&beam_id, &args.cmd).await?;
    std::process::exit(status.code().unwrap_or(1));
}
