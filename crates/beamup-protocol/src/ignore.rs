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

        // Always ignore .git directory and beamup internals
        let mut builder = GitignoreBuilder::new(root);
        let _ = builder.add_line(None, ".git");
        let _ = builder.add_line(None, ".beamup-tmp");
        let _ = builder.add_line(None, "*.beamup-tmp");
        if let Ok(gi) = builder.build() {
            rules.push(gi);
        }

        Self { rules }
    }

    pub fn is_ignored(&self, path: &Path, is_dir: bool) -> bool {
        for rule in &self.rules {
            let matched = rule.matched(path, is_dir);
            if matched.is_ignore() {
                return true;
            }
            if matched.is_whitelist() {
                return false;
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
