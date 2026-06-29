/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is dual-licensed under either the MIT license found in the
 * LICENSE-MIT file in the root directory of this source tree or the Apache
 * License, Version 2.0 found in the LICENSE-APACHE file in the root directory
 * of this source tree. You may select, at your option, one of the
 * above-listed licenses.
 */

use std::hash::Hash;
use std::hash::Hasher;
use std::sync::Arc;

use allocative::Allocative;
use buck2_common::external_symlink::ExternalSymlink;
use buck2_common::file_ops::metadata::FileDigest;
use buck2_common::file_ops::metadata::FileMetadata;
use buck2_core::content_hash::ContentBasedPathHash;
use buck2_directory::directory::entry::DirectoryEntry;
use buck2_util::strong_hasher::Blake3StrongHasher;
use dupe::Dupe;
use pagable::Pagable;
use strong_hash::StrongHash;

use crate::directory::ActionDirectoryEntry;
use crate::directory::ActionDirectoryMember;
use crate::directory::ActionSharedDirectory;

#[derive(Clone, Dupe, Debug, PartialEq, Eq, Allocative, Pagable)]
enum UnderlyingContentBasedPathHash {
    Inferred,
    Explicit(Arc<ContentBasedPathHash>),
}

/// `ArtifactValue` stores enough information about an artifact such that, if
/// it's in the CAS, we don't have to read anything from disk. In summary:
/// - for files, that's the digest and whether it's executable;
/// - for symlinks, that's its target (which we'd read with `fs::read_link`);
/// - for directories, that's the whole file tree.
///
/// However, when we have symlinks, we also must make the artifacts they point
/// to available. Therefore, when this represents a symlink, or a directory
/// with symlinks pointing outside such directory, we must also store the value
/// of the artifacts pointed to by those symlinks. That's the `deps` attribute.
#[derive(Clone, Debug, Dupe, PartialEq, Eq, Allocative, Pagable)]
pub struct ArtifactValue {
    /// The information about the artifact i.e. digest + is_executable if this
    /// is a file, the file tree if this is a directory, and so on.
    entry: ActionDirectoryEntry<ActionSharedDirectory>,
    /// A tree with all other artifacts which this value depends on. Unlike
    /// `entry` above, which is rooted at this artifact's path, `deps` is
    /// always rooted at the project root.
    deps: Option<ActionSharedDirectory>,
    /// The content-based path hash of the artifact. This is usually inferred,
    /// but in some cases (e.g. projected artifacts) it is explicitly provided.
    content_based_path_hash: UnderlyingContentBasedPathHash,
}

/// Commits to every part of an [`ArtifactValue`] that participates in equality.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct ArtifactValueFingerprint([u8; 32]);

impl AsRef<[u8]> for ArtifactValueFingerprint {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

impl TryFrom<&[u8]> for ArtifactValueFingerprint {
    type Error = buck2_error::Error;

    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        Ok(Self(value.try_into().map_err(|_| {
            buck2_error::internal_error!(
                "Artifact value fingerprint must contain 32 bytes, got {}",
                value.len()
            )
        })?))
    }
}

impl From<&ArtifactValue> for ArtifactValueFingerprint {
    fn from(value: &ArtifactValue) -> Self {
        let mut hasher = Blake3StrongHasher::new();
        value.strong_hash(&mut hasher);
        Self(*hasher.finalize().as_bytes())
    }
}

impl StrongHash for ArtifactValue {
    fn strong_hash<H: Hasher>(&self, state: &mut H) {
        fn write_bytes<H: Hasher>(state: &mut H, bytes: &[u8]) {
            state.write(&(bytes.len() as u64).to_le_bytes());
            state.write(bytes);
        }

        fn write_digest<H: Hasher>(state: &mut H, digest: &FileDigest) {
            state.write(&[digest.raw_digest().algorithm() as u8]);
            write_bytes(state, digest.raw_digest().as_bytes());
            state.write(&digest.size().to_le_bytes());
        }

        state.write(b"artifact-value-v1");
        match &self.entry {
            ActionDirectoryEntry::Dir(directory) => {
                state.write(&[0]);
                write_digest(state, directory.fingerprint().data());
            }
            ActionDirectoryEntry::Leaf(ActionDirectoryMember::File(file)) => {
                state.write(&[1]);
                write_digest(state, file.digest.data());
                state.write(&[u8::from(file.is_executable)]);
            }
            ActionDirectoryEntry::Leaf(ActionDirectoryMember::Symlink(symlink)) => {
                state.write(&[2]);
                write_bytes(state, symlink.target().as_str().as_bytes());
            }
            ActionDirectoryEntry::Leaf(ActionDirectoryMember::ExternalSymlink(symlink)) => {
                state.write(&[3]);
                write_bytes(state, symlink.target_str().as_bytes());
                write_bytes(state, symlink.remaining_path().as_str().as_bytes());
            }
        }
        match &self.deps {
            Some(deps) => {
                state.write(&[1]);
                write_digest(state, deps.fingerprint().data());
            }
            None => state.write(&[0]),
        }
        match &self.content_based_path_hash {
            UnderlyingContentBasedPathHash::Inferred => state.write(&[0]),
            UnderlyingContentBasedPathHash::Explicit(content_hash) => {
                state.write(&[1]);
                match content_hash.as_ref() {
                    ContentBasedPathHash::Specified(value) => {
                        state.write(&[0]);
                        write_bytes(state, value.as_bytes());
                    }
                    ContentBasedPathHash::OutputArtifact => state.write(&[1]),
                    ContentBasedPathHash::AqueryPlaceholder => state.write(&[2]),
                    ContentBasedPathHash::Scratch => state.write(&[3]),
                    ContentBasedPathHash::RelativePathResolution => state.write(&[4]),
                    ContentBasedPathHash::DepFilesPlaceholder => state.write(&[5]),
                }
            }
        }
    }
}

impl ArtifactValue {
    pub fn new(
        entry: ActionDirectoryEntry<ActionSharedDirectory>,
        deps: Option<ActionSharedDirectory>,
    ) -> Self {
        Self {
            entry,
            deps,
            content_based_path_hash: UnderlyingContentBasedPathHash::Inferred,
        }
    }

    pub fn file(meta: FileMetadata) -> Self {
        Self {
            entry: ActionDirectoryEntry::Leaf(ActionDirectoryMember::File(meta)),
            deps: None,
            content_based_path_hash: UnderlyingContentBasedPathHash::Inferred,
        }
    }

    pub fn dir(dir: ActionSharedDirectory) -> Self {
        Self {
            entry: ActionDirectoryEntry::Dir(dir),
            deps: None,
            content_based_path_hash: UnderlyingContentBasedPathHash::Inferred,
        }
    }

    pub fn is_dir(&self) -> bool {
        matches!(self.entry, ActionDirectoryEntry::Dir(_))
    }

    pub fn external_symlink(symlink: Arc<ExternalSymlink>) -> Self {
        Self {
            entry: ActionDirectoryEntry::Leaf(ActionDirectoryMember::ExternalSymlink(symlink)),
            deps: None,
            content_based_path_hash: UnderlyingContentBasedPathHash::Inferred,
        }
    }

    pub fn entry(&self) -> &ActionDirectoryEntry<ActionSharedDirectory> {
        &self.entry
    }

    pub fn deps(&self) -> Option<&ActionSharedDirectory> {
        self.deps.as_ref()
    }

    pub fn digest(&self) -> Option<&FileDigest> {
        match &self.entry {
            ActionDirectoryEntry::Dir(d) => Some(d.fingerprint().data()),
            ActionDirectoryEntry::Leaf(ActionDirectoryMember::File(f)) => Some(f.digest.data()),
            ActionDirectoryEntry::Leaf(ActionDirectoryMember::Symlink(..)) => None,
            ActionDirectoryEntry::Leaf(ActionDirectoryMember::ExternalSymlink(..)) => None,
        }
    }

    /// Size of this artifact (and its dependencies) in bytes.
    pub fn size(&self) -> u64 {
        (match &self.entry {
            DirectoryEntry::Dir(d) => d.size(),
            DirectoryEntry::Leaf(m) => m.size(),
        } + self.deps.as_ref().map_or(0, |d| d.size()))
    }

    pub fn with_content_based_path_hash(
        self,
        content_based_path_hash: ContentBasedPathHash,
    ) -> Self {
        Self {
            content_based_path_hash: UnderlyingContentBasedPathHash::Explicit(Arc::new(
                content_based_path_hash,
            )),
            ..self
        }
    }

    /// Distinguishes producer-supplied path identity from hashes inferred from artifact contents.
    ///
    /// Persistence must preserve this distinction so reloaded artifacts retain the producer's
    /// path semantics instead of freezing a transient inference as explicit state.
    pub fn explicit_content_based_path_hash(&self) -> Option<&ContentBasedPathHash> {
        match &self.content_based_path_hash {
            UnderlyingContentBasedPathHash::Inferred => None,
            UnderlyingContentBasedPathHash::Explicit(hash) => Some(hash),
        }
    }

    pub fn content_based_path_hash(&self) -> ContentBasedPathHash {
        if let UnderlyingContentBasedPathHash::Explicit(hash) = &self.content_based_path_hash {
            return (**hash).clone();
        }

        match &self.entry {
            ActionDirectoryEntry::Dir(d) => {
                ContentBasedPathHash::new(d.fingerprint().data().raw_digest().as_bytes())
            }
            ActionDirectoryEntry::Leaf(ActionDirectoryMember::File(f)) => {
                ContentBasedPathHash::new(f.digest.data().raw_digest().as_bytes())
            }
            ActionDirectoryEntry::Leaf(ActionDirectoryMember::Symlink(s)) => {
                let mut hasher = Blake3StrongHasher::new();
                s.target().hash(&mut hasher);
                ContentBasedPathHash::new(hasher.finalize().as_bytes())
            }
            ActionDirectoryEntry::Leaf(ActionDirectoryMember::ExternalSymlink(s)) => {
                let mut hasher = Blake3StrongHasher::new();
                s.hash(&mut hasher);
                ContentBasedPathHash::new(hasher.finalize().as_bytes())
            }
        }
        .expect("Constructed valid content-based path hash")
    }
}

#[cfg(test)]
mod tests {
    use buck2_common::cas_digest::CasDigestConfig;
    use buck2_common::cas_digest::DigestAlgorithmFamily;
    use buck2_common::file_ops::metadata::TrackedFileDigest;

    use super::*;

    #[test]
    fn persistent_fingerprint_distinguishes_values_with_the_same_path_hash() {
        let digest_config = CasDigestConfig::testing_default();
        let mut first_digest = [0; 20];
        first_digest[8] = 1;
        let mut second_digest = [0; 20];
        second_digest[8] = 2;
        let first = ArtifactValue::file(FileMetadata {
            digest: TrackedFileDigest::new(
                FileDigest::from_digest_bytes(DigestAlgorithmFamily::Sha1, &first_digest, 1)
                    .unwrap(),
                digest_config,
            ),
            is_executable: false,
        });
        let second = ArtifactValue::file(FileMetadata {
            digest: TrackedFileDigest::new(
                FileDigest::from_digest_bytes(DigestAlgorithmFamily::Sha1, &second_digest, 1)
                    .unwrap(),
                digest_config,
            ),
            is_executable: false,
        });

        assert_eq!(
            first.content_based_path_hash(),
            second.content_based_path_hash()
        );
        assert_ne!(
            ArtifactValueFingerprint::from(&first),
            ArtifactValueFingerprint::from(&second)
        );
    }
}
