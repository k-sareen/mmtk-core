//! Some information for the current MMTk build.

mod raw {
    // This includes a full list of the constants in built.rs generated by the 'built' crate.
    // https://docs.rs/built/latest/built/index.html
    include!(concat!(env!("OUT_DIR"), "/built.rs"));
}

/// MMTk crate version such as 0.14.0
pub const MMTK_PKG_VERSION: &str = raw::PKG_VERSION;

/// Comma separated features enabled for this build
pub const MMTK_FEATURES: &str = raw::FEATURES_STR;

lazy_static! {
    /// Git version as short commit hash, such as a96e8f9, or a96e8f9-dirty, or unknown-git-version if MMTk
    /// is not built from a git repo.
    pub static ref MMTK_GIT_VERSION: &'static str = &MMTK_GIT_VERSION_STRING;
    // Owned string
    static ref MMTK_GIT_VERSION_STRING: String = match (raw::GIT_COMMIT_HASH, raw::GIT_DIRTY) {
        (Some(hash), Some(dirty)) => format!("{}{}", hash.split_at(7).0, if dirty { "-dirty" } else { "" }),
        (Some(hash), None) => format!("{}{}", hash.split_at(7).0, "-?"),
        _ => "unknown-git-version".to_string(),
    };

    /// Full build info, including MMTk's name, version, git, and features in the build,
    /// such as MMTk 0.14.0 (43e0ce8-dirty, DEFAULT, EXTREME_ASSERTIONS)
    pub static ref MMTK_FULL_BUILD_INFO: &'static str = &MMTK_FULL_BUILD_INFO_STRING;
    // Owned string
    static ref MMTK_FULL_BUILD_INFO_STRING: String = format!("MMTk {} ({}, {})", MMTK_PKG_VERSION, *MMTK_GIT_VERSION, MMTK_FEATURES);
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_git_version() {
        println!("Git version: {}", *crate::build_info::MMTK_GIT_VERSION);
    }

    #[test]
    fn test_full_build_version() {
        println!(
            "Full build version: {}",
            *crate::build_info::MMTK_FULL_BUILD_INFO
        );
    }

    #[test]
    fn test_pkg_version() {
        println!("Package version: {}", crate::build_info::MMTK_PKG_VERSION);
    }

    #[test]
    fn test_features() {
        println!("Features: {}", crate::build_info::MMTK_FEATURES);
    }
}
