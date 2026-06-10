use std::path::{Path, PathBuf};

use ignore::gitignore::{Gitignore, GitignoreBuilder};

pub struct IgnoreRules {
    rules: Vec<Gitignore>,
}

impl IgnoreRules {
    pub fn load(root: &Path) -> Self {
        let mut rules = Vec::new();

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

        // Always ignore beamup internals and platform-specific git files
        let mut builder = GitignoreBuilder::new(root);
        let _ = builder.add_line(None, "*.beamup-tmp");
        let _ = builder.add_line(None, "*.beamup-pull-tmp");
        let _ = builder.add_line(None, "*.beamup-chunk-*");
        let _ = builder.add_line(None, "*.beamup-lz4");
        let _ = builder.add_line(None, "*.beamup-lz4-chunk-*");
        let _ = builder.add_line(None, "*.beamup-chunk-tmp");
        let _ = builder.add_line(None, ".git/index");
        let _ = builder.add_line(None, ".git/index.lock");
        let _ = builder.add_line(None, ".git/modules/**/index");
        let _ = builder.add_line(None, ".git/modules/**/index.lock");
        if let Ok(gi) = builder.build() {
            rules.push(gi);
        }

        Self { rules }
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
