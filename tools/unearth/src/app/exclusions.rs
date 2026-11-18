use regex::Regex;
use std::env;
use std::fs;
use std::path::{Path, PathBuf};

#[derive(Clone, Debug)]
enum Rule {
    Path(PathBuf),
    Name(std::ffi::OsString),
    Regex(Regex),
}

#[derive(Clone, Debug)]
pub(crate) struct Exclusions {
    rules: Vec<Rule>,
}

impl Exclusions {
    pub(crate) fn is_empty(&self) -> bool {
        self.rules.is_empty()
    }

    pub(crate) fn load() -> Result<Self, String> {
        let path = if let Some(config_home) =
            env::var_os("XDG_CONFIG_HOME").filter(|path| !path.is_empty())
        {
            PathBuf::from(config_home).join("fsx/excludes.tsv")
        } else {
            PathBuf::from(env::var_os("HOME").ok_or("HOME is not set")?)
                .join(".config/fsx/excludes.tsv")
        };
        match fs::read_to_string(&path) {
            Ok(contents) => Self::from_lines(&contents)
                .map_err(|error| format!("invalid exclusions file '{}': {error}", path.display())),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                Ok(Self { rules: Vec::new() })
            }
            Err(error) => Err(format!(
                "cannot read exclusions file '{}': {error}",
                path.display()
            )),
        }
    }

    fn from_lines(contents: &str) -> Result<Self, String> {
        let mut rules = Vec::new();
        for (line_number, line) in contents.lines().enumerate() {
            let line = line.trim_end_matches('\r');
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            let (kind, value) = line.split_once('\t').ok_or_else(|| {
                format!("line {} must contain a tab-separated rule", line_number + 1)
            })?;
            if value.is_empty() || value.contains('\t') {
                return Err(format!(
                    "line {} has an empty or invalid rule",
                    line_number + 1
                ));
            }
            let rule = match kind {
                "path" => {
                    let path = PathBuf::from(value);
                    if !path.is_absolute() {
                        return Err(format!("line {} path must be absolute", line_number + 1));
                    }
                    Rule::Path(path)
                }
                "name" => Rule::Name(value.into()),
                "regex" => Rule::Regex(Regex::new(value).map_err(|error| {
                    format!("line {} has an invalid regex: {error}", line_number + 1)
                })?),
                _ => return Err(format!("line {} has unknown rule type", line_number + 1)),
            };
            rules.push(rule);
        }
        Ok(Self { rules })
    }

    pub(crate) fn excludes(&self, root: &Path, path: &Path, is_dir: bool) -> bool {
        let mut ancestor = if is_dir {
            path
        } else {
            path.parent().unwrap_or(path)
        };
        loop {
            if self.rules.iter().any(|rule| match rule {
                Rule::Path(excluded) => ancestor == excluded,
                Rule::Name(name) => ancestor.file_name() == Some(name.as_os_str()),
                Rule::Regex(regex) => ancestor
                    .file_name()
                    .and_then(|name| name.to_str())
                    .is_some_and(|name| regex.is_match(name)),
            }) {
                return true;
            }
            if ancestor == root {
                return false;
            }
            let Some(parent) = ancestor.parent() else {
                return false;
            };
            if !parent.starts_with(root) {
                return false;
            }
            ancestor = parent;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::Exclusions;
    use std::path::Path;

    #[test]
    fn applies_path_name_and_regex_rules_to_directories_only() {
        let rules = Exclusions::from_lines("path\t/r/a/exact\nname\tSDK-kit\nregex\t^opaque$")
            .expect("valid exclusion rules");

        assert!(rules.excludes(Path::new("/r"), Path::new("/r/a/exact/child"), false));
        assert!(!rules.excludes(Path::new("/r"), Path::new("/r/a/exact"), false));
        assert!(rules.excludes(Path::new("/r"), Path::new("/r/a/SDK-kit"), true));
        assert!(!rules.excludes(Path::new("/r"), Path::new("/r/a/skd-kit"), true));
        assert!(rules.excludes(Path::new("/r"), Path::new("/r/a/opaque"), true));
        assert!(rules.excludes(Path::new("/r"), Path::new("/r/b/opaque"), true));
        assert!(!rules.excludes(Path::new("/r"), Path::new("/r/a/SDK-kit"), false));
    }

    #[test]
    fn a_git_file_is_not_a_directory_exclusion() {
        let rules = Exclusions::from_lines("name\t.git").expect("valid rules");
        assert!(rules.excludes(Path::new("/r"), Path::new("/r/.git/child"), false));
        assert!(!rules.excludes(Path::new("/r"), Path::new("/r/.git"), false));
    }

    #[test]
    fn malformed_regex_is_reported() {
        assert!(Exclusions::from_lines("regex\t[").is_err());
    }
}
