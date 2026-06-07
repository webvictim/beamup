use std::path::PathBuf;

use anyhow::Result;
use directories::ProjectDirs;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct Session {
    pub beam_id: String,
    pub local_dir: PathBuf,
    pub remote_dir: String,
}

impl Session {
    fn session_dir() -> Result<PathBuf> {
        let dirs = ProjectDirs::from("", "", "beamup")
            .ok_or_else(|| anyhow::anyhow!("cannot determine config directory"))?;
        let dir = dirs.data_dir().join("sessions");
        std::fs::create_dir_all(&dir)?;
        Ok(dir)
    }

    fn session_file() -> Result<PathBuf> {
        Ok(Self::session_dir()?.join("active.json"))
    }

    pub fn save(&self) -> Result<()> {
        let path = Self::session_file()?;
        let json = serde_json::to_string_pretty(self)?;
        std::fs::write(&path, json)?;
        Ok(())
    }

    pub fn load() -> Result<Option<Self>> {
        let path = Self::session_file()?;
        if !path.exists() {
            return Ok(None);
        }
        let json = std::fs::read_to_string(&path)?;
        let session: Self = serde_json::from_str(&json)?;
        Ok(Some(session))
    }

    pub fn remove() -> Result<()> {
        let path = Self::session_file()?;
        if path.exists() {
            std::fs::remove_file(&path)?;
        }
        Ok(())
    }
}
