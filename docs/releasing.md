# Releasing

[Intentional](https://github.com/wyrd-company/intentional) manages the
`nats-transport` release unit in `.intentional/config.yml`. Its committed
MSBuild projection is
`src/Community.NServiceBus.Transport.Nats/Community.NServiceBus.Transport.Nats.csproj`.
The test projects are excluded from the release inventory.

Install the same version used by CI:

```bash
npm install --global @wyrd-company/intentional@0.4.1
```

Existing bare SemVer tags, including `2.0.0`, supply released-version authority.
Keep this tag stream: do not add a `v` prefix or recreate an existing tag.
Use a full checkout with tags when running Intentional.

## Record changes

Commit an intent with each consumer-facing change:

```bash
intentional add --release-unit nats-transport:patch --message "Describe the fix for users."
intentional status
intentional check
```

Use `patch` for fixes, `minor` for new capabilities, and `major` for breaking
changes. CI validates the configuration, intents, and committed version against
the release tags. CI and documentation maintenance alone do not need an intent.

## Prepare a release

Start from an up-to-date, clean branch with the approved intents. Save the
canonical plan outside the worktree because it is needed after `apply` consumes
the intents:

```bash
intentional status
intentional check
intentional plan > ../nats-release-plan.json
intentional apply --dry-run
intentional apply
```

Review the projected version, the consumed intents, and the generated changelog
at `src/Community.NServiceBus.Transport.Nats/CHANGELOG.md`. Build and test the
solution against the NATS version documented in the README:

```bash
dotnet build src/Community.NServiceBus.Transport.Nats.slnx --configuration Release
dotnet test src/Community.NServiceBus.Transport.Nats.slnx --configuration Release --no-build
intentional check
```

Commit the applied release through the repository's normal operator workflow
and merge that exact commit into `main`. Keep the saved plan unchanged; its
digest binds the release records to the reviewed changes. Then preview and
create the annotated release tag at that commit:

```bash
intentional tag --plan ../nats-release-plan.json --dry-run
intentional tag --plan ../nats-release-plan.json
intentional status
intentional check
```

Push the generated tag only when publication is intended. The existing Release
workflow validates Intentional state, builds the committed package version,
runs all three test suites, publishes to NuGet, and creates a GitHub Release.
Intentional does not commit, push, or publish packages itself.
