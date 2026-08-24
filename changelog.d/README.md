# Pending changelog fragments

One file per change, so concurrent branches never conflict the way shared
`CHANGELOG.md` edits do. At release time `npm version` folds every fragment
here into the new version section of `CHANGELOG.md` and deletes it.

**Name:** `<slug>.<breaking|added|changed|fixed>.md` — the slug just has to
be unique among pending fragments; the PR number or branch name works
(`10-ci-workflow.added.md`).

**Content:** one or more markdown bullets, exactly as they should appear in
`CHANGELOG.md`:

```markdown
- `listen` streams messages from monitored channels as they arrive (#7).
  Continuation lines indent two spaces.
```

Breaking fragments open by naming who needs to act:
`- **Host configs:** …`.

**What needs an entry:** anything a user of the server would notice — tools,
their schemas or output shapes, monitoring behavior, configuration, defaults.
Internal refactors, test-only, and docs-only changes don't (apply the
`no-changelog` label on PRs).
