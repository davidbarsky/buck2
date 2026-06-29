/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is dual-licensed under either the MIT license found in the
 * LICENSE-MIT file in the root directory of this source tree or the Apache
 * License, Version 2.0 found in the LICENSE-APACHE file in the root directory
 * of this source tree. You may select, at your option, one of the
 * above-listed licenses.
 */

use std::ffi::OsStr;
use std::ffi::OsString;
use std::sync::OnceLock;

use buck2_fs::paths::abs_path::AbsPath;
use dupe::Dupe;

pub const BUCK_BUILD_ID: &str = "BUCK_BUILD_ID";
pub const BUCK2_DAEMON_UUID: &str = "BUCK2_DAEMON_UUID";

/// Invocation identity injected after the action's declared and inherited environment.
#[derive(Debug, Eq, PartialEq)]
pub struct LocalExecutionIdentity {
    pub buck_build_id: BuckBuildId,
    pub buck2_daemon_uuid: Buck2DaemonUuid,
}

#[derive(Debug, Eq, PartialEq)]
pub struct BuckBuildId(pub String);

#[derive(Debug, Eq, PartialEq)]
pub struct Buck2DaemonUuid(pub String);

impl<'a> IntoIterator for &'a LocalExecutionIdentity {
    type Item = (&'static str, &'a str);
    type IntoIter = std::array::IntoIter<Self::Item, 2>;

    fn into_iter(self) -> Self::IntoIter {
        [
            (BUCK_BUILD_ID, self.buck_build_id.0.as_str()),
            (BUCK2_DAEMON_UUID, self.buck2_daemon_uuid.0.as_str()),
        ]
        .into_iter()
    }
}

#[cfg(fbcode_build)]
const ENV_ALLOW_LIST: &[&str] = test_env_allowlist::LEGACY_TESTPILOT_ALLOW_LIST;

#[cfg(all(unix, not(fbcode_build)))]
const ENV_ALLOW_LIST: &[&str] = &[
    "PATH",
    "USER",
    "LOGNAME",
    "HOME",
    "TMPDIR",
    // Generally needed to keep systemd working
    "XDG_RUNTIME_DIR",
];

// The standard (built-in) variables.
// https://ss64.com/nt/syntax-variables.html
#[cfg(all(windows, not(fbcode_build)))]
const ENV_ALLOW_LIST: &[&str] = &[
    "ALLUSERSPROFILE",
    "APPDATA",
    "COMPUTERNAME",
    "COMSPEC",
    "CommonProgramFiles",
    "CommonProgramFiles(x86)",
    "HOMEDRIVE",
    "HOMEPATH",
    "LOCALAPPDATA",
    "NUMBER_OF_PROCESSORS",
    "OS",
    "PATH",
    "PATHEXT",
    "PROCESSOR_ARCHITECTURE",
    "PROCESSOR_ARCHITEW6432",
    "PROCESSOR_IDENTIFIER",
    "PROCESSOR_LEVEL",
    "PROCESSOR_REVISION",
    "PSModulePath",
    "ProgramData",
    "ProgramFiles",
    "ProgramFiles(x86)",
    "ProgramW6432",
    "Public",
    "SYSTEMDRIVE",
    "SYSTEMROOT",
    "TEMP",
    "TMP",
    "USERDOMAIN",
    "USERNAME",
    "USERPROFILE",
    "UserDnsDomain",
    "WINDIR",
];

#[derive(Copy, Clone, Dupe, Debug)]
pub struct EnvironmentInheritance {
    clear: bool,
    values: &'static [(&'static str, OsString)],
    exclusions: &'static [&'static str],
}

impl EnvironmentInheritance {
    pub fn test_allowlist() -> Self {
        // This is made to be a list of lists in case we want to include lists from different
        // provenances, like the test_env_allowlist::ENV_LIST_HACKY.
        let allowlists = &[ENV_ALLOW_LIST];

        // We create this *once* since getenv is actually not cheap (being O(n) of the environment
        // size).
        static TEST_CELL: OnceLock<Vec<(&'static str, OsString)>> = OnceLock::new();

        let values = TEST_CELL.get_or_init(|| {
            let mut ret = Vec::new();
            for list in allowlists.iter() {
                for key in list.iter() {
                    if let Some(value) = std::env::var_os(key) {
                        ret.push((*key, value));
                    }
                }
            }
            ret
        });

        Self {
            clear: true,
            values,
            exclusions: &[],
        }
    }

    /// Exclude some vars that are known to cause issues. In an ideal world we should do a
    /// migration to lock this down everywhere.
    pub fn local_command_exclusions() -> Self {
        Self {
            clear: false,
            values: &[],
            exclusions: &[
                "PYTHONPATH",
                "PYTHONHOME",
                "PYTHONSTARTUP",
                "LD_LIBRARY_PATH",
                "LD_PRELOAD",
            ],
        }
    }

    pub fn empty() -> Self {
        Self {
            values: &[],
            exclusions: &[],
            clear: true,
        }
    }

    pub fn values(&self) -> impl Iterator<Item = (&'static str, &'static OsString)> + use<> {
        self.values.iter().map(|(k, v)| (*k, v))
    }

    pub fn exclusions(&self) -> impl Iterator<Item = &'static str> + use<> {
        self.exclusions.iter().copied()
    }

    pub fn clear(&self) -> bool {
        self.clear
    }
}

/// Visits directives in execution order so execution and reuse hashing see the same environment.
pub fn visit_local_execution_environment(
    working_directory: &AbsPath,
    environment: impl IntoIterator<Item = (impl AsRef<OsStr>, impl AsRef<OsStr>)>,
    inheritance: Option<&EnvironmentInheritance>,
    mut visit: impl FnMut(LocalEnvironmentDirective<'_>),
) {
    if let Some(inheritance) = inheritance {
        if inheritance.clear() {
            visit(LocalEnvironmentDirective::Clear);
        }

        for key in inheritance.exclusions() {
            visit(LocalEnvironmentDirective::Remove(OsStr::new(key)));
        }

        for (key, value) in inheritance.values() {
            visit(LocalEnvironmentDirective::Set {
                key: OsStr::new(key),
                value,
            });
        }
    }
    for (key, value) in environment {
        visit(LocalEnvironmentDirective::Set {
            key: key.as_ref(),
            value: value.as_ref(),
        });
    }
    visit(LocalEnvironmentDirective::Set {
        key: OsStr::new("PWD"),
        value: working_directory.as_os_str(),
    });
}

/// Decouples environment reuse hashing from any particular process builder.
pub enum LocalEnvironmentDirective<'a> {
    Clear,
    Remove(&'a OsStr),
    Set { key: &'a OsStr, value: &'a OsStr },
}
