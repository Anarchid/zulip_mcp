# Pending changelog fragments

One file per change, so concurrent branches never conflict the way shared
`CHANGELOG.md` edits do. At release time `npm version` folds every fragment
here into the new version section of `CHANGELOG.md` and deletes it.

**Name:** `<slug>.<breaking|added|changed|fixed>.md`, as a flat file directly
in this directory. The slug just has to be unique among pending fragments and
filesystem-safe: the PR number works, and so does the branch name with `/`
replaced by `-` (`10-ci-workflow.added.md`, `fix-retry-backoff.fixed.md`). The
release script refuses subdirectories and any other stray file here, so a
misplaced entry fails the release loudly instead of being left out.

**Content:** one or more markdown bullets, exactly as they should appear in
`CHANGELOG.md`. Continuation lines indent two spaces (nested bullets are
fine); headings and horizontal rules are refused even when indented, since a
heading inside a fragment would corrupt the section structure:


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
