use std::path::{Path, PathBuf};

use anyhow::{bail, Context};
use ignore::gitignore::{Gitignore, GitignoreBuilder};

/// Never synced, and checked before `--exclude`, `.gitignore` and `.beamignore`, so a
/// `!` pattern in those can't bring them back.
const BUILTIN_PATTERNS: &[&str] = &[
    // beamup's own in-flight transfer files.
    "*.beamup-tmp",
    "*.beamup-pull-tmp",
    "*.beamup-chunk-*",
    "*.beamup-lz4",
    "*.beamup-lz4-chunk-*",
    "*.beamup-chunk-tmp",
    // Platform-specific binary formats.
    ".git/index",
    ".git/index.lock",
    ".git/modules/**/index",
    ".git/modules/**/index.lock",
    // tsh profiles. A beam's certificate is only usable with the private key its
    // own tbot holds in memory, so a profile carried to another beam (or baked
    // into a Phase snapshot) makes tsh sign with the wrong key, and every Beam
    // command there fails with "tls: error decrypting message".
    ".tsh/",
    // kubeconfigs written by tsh point at the local tsh profile and this beam's
    // cluster. Syncing one either way leaves the other side with the wrong
    // credentials, or overwrites a laptop's real ~/.kube/config.
    ".kube/",
    // Caches: large, rebuildable, and full of platform-specific output.
    ".cache/",
];

pub struct IgnoreRules {
    rules: Vec<Gitignore>,
}

impl IgnoreRules {
    /// Rules for `root`: the built-ins, then `.gitignore` and `.beamignore`.
    pub fn load(root: &Path) -> Self {
        Self::with_excludes(root, &[]).expect("built-in ignore patterns are valid")
    }

    /// Like [`IgnoreRules::load`], plus `--exclude` patterns (gitignore syntax,
    /// relative to `root`). Excludes are checked after the built-ins and before the
    /// files, so a `!` line in `.gitignore` or `.beamignore` can't undo one.
    pub fn with_excludes(root: &Path, excludes: &[String]) -> anyhow::Result<Self> {
        let mut rules = Vec::new();

        let mut builder = GitignoreBuilder::new(root);
        for pattern in BUILTIN_PATTERNS {
            builder.add_line(None, pattern)?;
        }
        rules.push(builder.build()?);

        if !excludes.is_empty() {
            let mut builder = GitignoreBuilder::new(root);
            for pattern in excludes {
                validate_exclude(pattern)?;
                builder
                    .add_line(None, pattern)
                    .with_context(|| format!("invalid --exclude pattern {pattern:?}"))?;
            }
            rules.push(builder.build().context("invalid --exclude patterns")?);
        }

        // Load .gitignore
        let gitignore_path = root.join(".gitignore");
        if gitignore_path.exists() {
            let mut builder = GitignoreBuilder::new(root);
            builder.add(&gitignore_path);
            if let Ok(gi) = builder.build() {
                rules.push(gi);
            }
        }

        // Load .beamignore
        let beamignore_path = root.join(".beamignore");
        if beamignore_path.exists() {
            let mut builder = GitignoreBuilder::new(root);
            builder.add(&beamignore_path);
            if let Ok(gi) = builder.build() {
                rules.push(gi);
            }
        }

        Ok(Self { rules })
    }

    pub fn is_ignored(&self, path: &Path, is_dir: bool) -> bool {
        // Check the path itself and all ancestor directories
        // This ensures that files inside an ignored directory are also ignored
        let mut current = PathBuf::new();
        for component in path.components() {
            current.push(component);
            let check_is_dir = if current == path { is_dir } else { true };
            for rule in &self.rules {
                let matched = rule.matched(&current, check_is_dir);
                if matched.is_ignore() {
                    return true;
                }
                if matched.is_whitelist() {
                    return false;
                }
            }
        }
        false
    }

    pub fn filter_path(&self, root: &Path, full_path: &Path, is_dir: bool) -> bool {
        let relative = full_path.strip_prefix(root).unwrap_or(full_path);
        self.is_ignored(relative, is_dir)
    }
}

pub fn relative_path(root: &Path, full_path: &Path) -> PathBuf {
    full_path.strip_prefix(root).unwrap_or(full_path).to_path_buf()
}

/// Reject `--exclude` values that gitignore would silently treat as something
/// other than an exclusion.
fn validate_exclude(pattern: &str) -> anyhow::Result<()> {
    let trimmed = pattern.trim();
    if trimmed.is_empty() {
        bail!("--exclude pattern is empty");
    }
    if trimmed.starts_with('!') {
        bail!("--exclude pattern {pattern:?} starts with '!'; negations belong in .beamignore");
    }
    if trimmed.starts_with('#') {
        bail!("--exclude pattern {pattern:?} starts with '#', which gitignore reads as a comment; write \\# to match a literal '#'");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    fn setup_test_dir(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("beamup-test-{name}"));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn ignores_beamup_temp_files() {
        let root = setup_test_dir("ignore-beamup-tmp");
        let rules = IgnoreRules::load(&root);

        assert!(rules.is_ignored(Path::new("file.beamup-tmp"), false));
        assert!(rules.is_ignored(Path::new("data.beamup-chunk-0001"), false));
        assert!(rules.is_ignored(Path::new("data.beamup-lz4"), false));
        assert!(rules.is_ignored(Path::new("data.beamup-lz4-chunk-0003"), false));
        assert!(rules.is_ignored(Path::new("foo.beamup-chunk-tmp"), false));

        let _ = fs::remove_dir_all(&root);
    }

    #[test]
    fn does_not_ignore_normal_files() {
        let root = setup_test_dir("ignore-normal");
        let rules = IgnoreRules::load(&root);

        assert!(!rules.is_ignored(Path::new("src/main.rs"), false));
        assert!(!rules.is_ignored(Path::new("Cargo.toml"), false));
        assert!(!rules.is_ignored(Path::new("README.md"), false));

        let _ = fs::remove_dir_all(&root);
    }

    #[test]
    fn gitignore_target_directory() {
        let root = setup_test_dir("ignore-target");
        fs::write(root.join(".gitignore"), "/target\n").unwrap();

        let rules = IgnoreRules::load(&root);

        assert!(rules.is_ignored(Path::new("target"), true));
        assert!(rules.is_ignored(Path::new("target/debug/deps/foo.o"), false));
        assert!(rules.is_ignored(Path::new("target/release/beamup"), false));
        assert!(rules.is_ignored(Path::new("target/aarch64-unknown-linux-musl/debug/beamup-agent"), false));
        assert!(rules.is_ignored(Path::new("target/x86_64-unknown-linux-musl/release/beamup-agent"), false));

        assert!(!rules.is_ignored(Path::new("target_config.toml"), false));

        let _ = fs::remove_dir_all(&root);
    }

    #[test]
    fn gitignore_node_modules() {
        let root = setup_test_dir("ignore-node-modules");
        fs::write(root.join(".gitignore"), "node_modules\n").unwrap();

        let rules = IgnoreRules::load(&root);

        assert!(rules.is_ignored(Path::new("node_modules"), true));
        assert!(rules.is_ignored(Path::new("node_modules/express/index.js"), false));
        assert!(rules.is_ignored(Path::new("node_modules/.package-lock.json"), false));

        let _ = fs::remove_dir_all(&root);
    }

    #[test]
    fn gitignore_wildcard_patterns() {
        let root = setup_test_dir("ignore-wildcards");
        fs::write(root.join(".gitignore"), "*.log\n*.tmp\n").unwrap();

        let rules = IgnoreRules::load(&root);

        assert!(rules.is_ignored(Path::new("app.log"), false));
        assert!(rules.is_ignored(Path::new("debug.tmp"), false));
        assert!(rules.is_ignored(Path::new("logs/server.log"), false));
        assert!(!rules.is_ignored(Path::new("app.rs"), false));

        let _ = fs::remove_dir_all(&root);
    }

    #[test]
    fn beamignore_file() {
        let root = setup_test_dir("ignore-beamignore");
        fs::write(root.join(".beamignore"), "data/\n*.sqlite\n").unwrap();

        let rules = IgnoreRules::load(&root);

        assert!(rules.is_ignored(Path::new("data"), true));
        assert!(rules.is_ignored(Path::new("data/big.bin"), false));
        assert!(rules.is_ignored(Path::new("app.sqlite"), false));
        assert!(!rules.is_ignored(Path::new("src/main.rs"), false));

        let _ = fs::remove_dir_all(&root);
    }

    #[test]
    fn filter_path_strips_prefix() {
        let root = setup_test_dir("ignore-filter-path");
        fs::write(root.join(".beamignore"), ".git\n").unwrap();
        let rules = IgnoreRules::load(&root);

        let full_path = root.join(".git/HEAD");
        assert!(rules.filter_path(&root, &full_path, false));

        let full_path = root.join("src/main.rs");
        assert!(!rules.filter_path(&root, &full_path, false));

        let _ = fs::remove_dir_all(&root);
    }

    #[test]
    fn ignores_git_index_files() {
        let root = setup_test_dir("ignore-git-index");
        let rules = IgnoreRules::load(&root);

        assert!(rules.is_ignored(Path::new(".git/index"), false));
        assert!(rules.is_ignored(Path::new(".git/index.lock"), false));
        assert!(rules.is_ignored(Path::new(".git/modules/e/index"), false));
        assert!(rules.is_ignored(Path::new(".git/modules/e/index.lock"), false));
        assert!(rules.is_ignored(Path::new(".git/modules/deep/nested/index"), false));
        assert!(rules.is_ignored(Path::new(".git/modules/deep/nested/index.lock"), false));

        // Shouldn't block other .git files
        assert!(!rules.is_ignored(Path::new(".git/HEAD"), false));
        assert!(!rules.is_ignored(Path::new(".git/config"), false));
        assert!(!rules.is_ignored(Path::new(".git/refs/heads/main"), false));

        let _ = fs::remove_dir_all(&root);
    }

    #[test]
    fn ignores_tsh_profiles() {
        let root = setup_test_dir("ignore-tsh");
        let rules = IgnoreRules::load(&root);

        // ~/.tsh when the sync root is the home directory...
        assert!(rules.is_ignored(Path::new(".tsh"), true));
        assert!(rules.is_ignored(Path::new(".tsh/keys/odd-firefly.beams.sh/bot-beam-x-x509.pem"), false));
        assert!(rules.is_ignored(Path::new(".tsh/odd-firefly.beams.sh.yaml"), false));
        // ...and a home directory's .tsh when the sync root is above it.
        assert!(rules.is_ignored(Path::new("beams/.tsh/current-profile"), false));

        // Only the directory, and only that name.
        assert!(!rules.is_ignored(Path::new(".tsh"), false));
        assert!(!rules.is_ignored(Path::new(".tsh.from-pi"), true));
        assert!(!rules.is_ignored(Path::new("scripts/setup.tsh"), false));
        assert!(!rules.is_ignored(Path::new("docs/tsh/usage.md"), false));

        let _ = fs::remove_dir_all(&root);
    }

    #[test]
    fn ignores_kube_and_cache_dirs() {
        let root = setup_test_dir("ignore-kube-cache");
        let rules = IgnoreRules::load(&root);

        assert!(rules.is_ignored(Path::new(".kube"), true));
        assert!(rules.is_ignored(Path::new(".kube/config"), false));
        assert!(rules.is_ignored(Path::new("beams/.kube/cache/discovery/x.json"), false));
        assert!(rules.is_ignored(Path::new(".cache"), true));
        assert!(rules.is_ignored(Path::new(".cache/pip/http/x"), false));
        // Project-level caches too.
        assert!(rules.is_ignored(Path::new("web/node_modules/.cache/babel/x"), false));
        assert!(rules.is_ignored(Path::new("project/.cache/x"), false));

        // Only directories with exactly those names.
        assert!(!rules.is_ignored(Path::new(".kube"), false));
        assert!(!rules.is_ignored(Path::new(".cache"), false));
        assert!(!rules.is_ignored(Path::new(".kube-backup/config"), false));
        assert!(!rules.is_ignored(Path::new(".pytest_cache/v/x"), false));
        assert!(!rules.is_ignored(Path::new("src/cache/mod.rs"), false));
        assert!(!rules.is_ignored(Path::new("deploy/kube/app.yaml"), false));

        let _ = fs::remove_dir_all(&root);
    }

    #[test]
    fn builtins_cannot_be_unignored() {
        let root = setup_test_dir("ignore-builtin-whitelist");
        fs::write(root.join(".gitignore"), "!.tsh/\n!*.beamup-tmp\n!.kube/\n").unwrap();
        fs::write(root.join(".beamignore"), "!.tsh\n!.cache\n").unwrap();

        let rules = IgnoreRules::load(&root);

        assert!(rules.is_ignored(Path::new(".tsh/keys/x"), false));
        assert!(rules.is_ignored(Path::new("file.beamup-tmp"), false));
        assert!(rules.is_ignored(Path::new(".kube/config"), false));
        assert!(rules.is_ignored(Path::new(".cache/x"), false));

        let _ = fs::remove_dir_all(&root);
    }

    #[test]
    fn excludes_are_applied() {
        let root = setup_test_dir("ignore-excludes");
        let excludes = vec!["*.sqlite".to_string(), "/build/".to_string(), ".kube".to_string()];
        let rules = IgnoreRules::with_excludes(&root, &excludes).unwrap();

        assert!(rules.is_ignored(Path::new("app.sqlite"), false));
        assert!(rules.is_ignored(Path::new("data/cache.sqlite"), false));
        assert!(rules.is_ignored(Path::new("build"), true));
        assert!(rules.is_ignored(Path::new("build/out.o"), false));
        assert!(rules.is_ignored(Path::new(".kube/config"), false));
        // Anchored: only the top-level build directory.
        assert!(!rules.is_ignored(Path::new("src/build/mod.rs"), false));
        assert!(!rules.is_ignored(Path::new("src/main.rs"), false));
        // Built-ins still apply alongside them.
        assert!(rules.is_ignored(Path::new(".tsh/keys/x"), false));

        let _ = fs::remove_dir_all(&root);
    }

    #[test]
    fn excludes_cannot_be_unignored_by_files() {
        let root = setup_test_dir("ignore-excludes-whitelist");
        fs::write(root.join(".gitignore"), "!secrets/\n").unwrap();
        let rules = IgnoreRules::with_excludes(&root, &["secrets/".to_string()]).unwrap();

        assert!(rules.is_ignored(Path::new("secrets/token"), false));

        let _ = fs::remove_dir_all(&root);
    }

    #[test]
    fn excludes_combine_with_ignore_files() {
        let root = setup_test_dir("ignore-excludes-files");
        fs::write(root.join(".gitignore"), "/target\n").unwrap();
        fs::write(root.join(".beamignore"), "*.log\n").unwrap();
        let rules = IgnoreRules::with_excludes(&root, &["*.csv".to_string()]).unwrap();

        assert!(rules.is_ignored(Path::new("target/debug/app"), false));
        assert!(rules.is_ignored(Path::new("run.log"), false));
        assert!(rules.is_ignored(Path::new("results.csv"), false));
        assert!(!rules.is_ignored(Path::new("src/lib.rs"), false));

        let _ = fs::remove_dir_all(&root);
    }

    #[test]
    fn filter_path_applies_excludes() {
        let root = setup_test_dir("ignore-excludes-filter-path");
        let rules = IgnoreRules::with_excludes(&root, &["node_modules".to_string()]).unwrap();

        assert!(rules.filter_path(&root, &root.join("web/node_modules/x/index.js"), false));
        assert!(!rules.filter_path(&root, &root.join("web/src/index.js"), false));

        let _ = fs::remove_dir_all(&root);
    }

    #[test]
    fn rejects_bad_excludes() {
        let root = setup_test_dir("ignore-excludes-invalid");
        for bad in ["", "   ", "!keep.txt", "# comment", "**/{a", "a[z-a]"] {
            assert!(
                IgnoreRules::with_excludes(&root, &[bad.to_string()]).is_err(),
                "expected {bad:?} to be rejected"
            );
        }
        // An escaped '#' is a real pattern.
        assert!(IgnoreRules::with_excludes(&root, &["\\#notes".to_string()]).is_ok());

        let _ = fs::remove_dir_all(&root);
    }

    #[test]
    fn relative_path_strips_prefix() {
        let root = Path::new("/home/user/project");
        let full = Path::new("/home/user/project/src/main.rs");
        assert_eq!(relative_path(root, full), PathBuf::from("src/main.rs"));
    }

    #[test]
    fn relative_path_returns_as_is_when_no_prefix() {
        let root = Path::new("/home/user/project");
        let full = Path::new("/other/path/file.rs");
        assert_eq!(relative_path(root, full), PathBuf::from("/other/path/file.rs"));
    }
}
