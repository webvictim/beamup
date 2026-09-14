use std::path::PathBuf;

/// Agent target triples we may embed, keyed by the `uname -m` value they run on.
const AGENT_TARGETS: &[(&str, &str)] = &[
    ("x86_64", "x86_64-unknown-linux-musl"),
    ("aarch64", "aarch64-unknown-linux-musl"),
];

fn main() {
    println!("cargo:rerun-if-env-changed=BEAMUP_AGENT_PATH");
    println!("cargo:rerun-if-changed=build.rs");

    let out_dir = PathBuf::from(std::env::var("OUT_DIR").unwrap());
    let mut embedded = Vec::new();

    for (arch, target) in AGENT_TARGETS {
        println!("cargo:rerun-if-env-changed=BEAMUP_AGENT_PATH_{}", arch.to_uppercase());

        let dest = out_dir.join(format!("beamup-agent-{arch}"));

        match find_agent_binary(arch, target) {
            Some(path) => {
                std::fs::copy(&path, &dest).expect("failed to copy agent binary to OUT_DIR");
                println!("cargo:rerun-if-changed={}", path.display());
                let len = std::fs::metadata(&dest).unwrap().len();
                eprintln!("beamup build: embedding {arch} agent from {} ({len} bytes)", path.display());
                embedded.push(*arch);
            }
            None => {
                // Empty placeholder so include_bytes! still compiles.
                std::fs::write(&dest, b"").unwrap();
                eprintln!("beamup build: no {arch} agent found, embedding disabled for {arch}");
            }
        }
    }

    if embedded.is_empty() {
        eprintln!("beamup build: WARNING no agent binaries embedded, will use runtime lookup");
    }
}

/// Architecture of an ELF binary, read from `e_machine` in its header.
///
/// The agent is always a Linux ELF, so this tells us which slot a binary belongs
/// in regardless of what host we're building on. Embedding an agent under the
/// wrong arch makes it die with "Exec format error" on the beam, which surfaces
/// only as a failed handshake — so we check rather than assume.
fn elf_arch(path: &std::path::Path) -> Option<&'static str> {
    let bytes = std::fs::read(path).ok()?;
    if bytes.len() < 20 || &bytes[0..4] != b"\x7fELF" {
        return None;
    }
    // e_machine: u16 at offset 18, endianness per EI_DATA (byte 5).
    let e_machine = match bytes[5] {
        1 => u16::from_le_bytes([bytes[18], bytes[19]]),
        2 => u16::from_be_bytes([bytes[18], bytes[19]]),
        _ => return None,
    };
    match e_machine {
        0x3E => Some("x86_64"),
        0xB7 => Some("aarch64"),
        _ => None,
    }
}

/// `BEAMUP_AGENT_PATH_<ARCH>` overrides a specific arch. The older, single-arch
/// `BEAMUP_AGENT_PATH` is matched by the ELF arch of the binary it points at, so
/// it lands in the right slot no matter which host is doing the build.
fn find_agent_binary(arch: &str, target: &str) -> Option<PathBuf> {
    let arch_var = format!("BEAMUP_AGENT_PATH_{}", arch.to_uppercase());
    if let Ok(path) = std::env::var(&arch_var) {
        let p = PathBuf::from(path);
        if p.exists() {
            match elf_arch(&p) {
                Some(found) if found != arch => {
                    panic!("{arch_var} points at a {found} binary, expected {arch}: {}", p.display());
                }
                _ => return Some(p),
            }
        }
    }

    if let Ok(path) = std::env::var("BEAMUP_AGENT_PATH") {
        let p = PathBuf::from(path);
        if p.exists() {
            match elf_arch(&p) {
                Some(found) if found == arch => return Some(p),
                Some(_) => {} // a different arch's agent; leave it for that slot
                None => eprintln!(
                    "beamup build: BEAMUP_AGENT_PATH={} is not a recognised Linux ELF, ignoring",
                    p.display()
                ),
            }
        }
    }

    for profile in ["release", "debug"] {
        let p = PathBuf::from(format!("../../target/{target}/{profile}/beamup-agent"));
        if p.exists() {
            return Some(p.canonicalize().unwrap_or(p));
        }
    }

    None
}
