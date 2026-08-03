# Releasing Agent World

GitHub Releases are immutable, named delivery points built from Git tags. The release tag, Cargo package version, downloadable archive, checksum, and evidence files must all describe the same commit.

## Current release policy

- Publish prereleases only, using semantic versions such as `0.1.0-alpha.1` and tags such as `v0.1.0-alpha.1`.
- Treat every published tag as immutable. Never move or reuse it.
- The current Windows archive is portable and unsigned. Its filename says so explicitly.
- Do not publish a stable version until Windows code signing, install/update/rollback, uninstall, resource, accessibility, authenticated-provider, and process-containment gates are complete.

## Publish a prerelease

1. Start from a clean, tested `main` branch.
2. Update `version` in `Cargo.toml` and refresh `Cargo.lock`.
3. Run the normal Windows CI checks.
4. Commit and push the version change.
5. Create and push an annotated tag that exactly matches the Cargo version:

   ```powershell
   git tag -a v0.1.0-alpha.1 -m "Agent World v0.1.0-alpha.1"
   git push origin v0.1.0-alpha.1
   ```

The `release` workflow then validates the tag, rebuilds and retests the tagged commit on Windows, packages `agent-world.exe`, writes a SHA-256 checksum, captures both self-check reports, generates release notes, and publishes a GitHub prerelease.

## Version choices

- Increment the prerelease suffix for another candidate: `0.1.0-alpha.2`.
- Use a beta after the alpha acceptance criteria are met: `0.1.0-beta.1`.
- Use a patch version for compatible fixes after a stable release: `0.1.1`.
- Use a minor version for compatible features: `0.2.0`.
- Use a major version for incompatible public behavior after the project reaches `1.0.0`.

The workflow deliberately rejects stable-looking tags while the signed-distribution gate remains open.
