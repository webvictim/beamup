use std::path::PathBuf;

fn main() {
    println!("cargo:rerun-if-env-changed=BEAMUP_AGENT_PATH");
    println!("cargo:rerun-if-changed=build.rs");

    let out_dir = std::env::var("OUT_DIR").unwrap();
    let dest = PathBuf::from(&out_dir).join("beamup-agent-embedded");

    let agent_path = find_agent_binary();

    if let Some(path) = agent_path {
        std::fs::copy(&path, &dest).expect("failed to copy agent binary to OUT_DIR");
        println!("cargo:rerun-if-changed={}", path.display());
        eprintln!("beamup build: embedding agent binary from {} ({} bytes)", path.display(), std::fs::metadata(&dest).unwrap().len());
    } else {
        // Write empty placeholder so include_bytes! doesn't fail
        std::fs::write(&dest, b"").unwrap();
        eprintln!("beamup build: no agent binary found, embedding disabled (will use runtime lookup)");
    }
}

fn find_agent_binary() -> Option<PathBuf> {
    if let Ok(path) = std::env::var("BEAMUP_AGENT_PATH") {
        let p = PathBuf::from(&path);
        if p.exists() {
            return Some(p);
        }
    }

    let candidates = [
        "../../target/aarch64-unknown-linux-musl/release/beamup-agent",
        "../../target/aarch64-unknown-linux-musl/debug/beamup-agent",
    ];
    for candidate in &candidates {
        let p = PathBuf::from(candidate);
        if p.exists() {
            return Some(p.canonicalize().unwrap_or(p));
        }
    }

    None
}
