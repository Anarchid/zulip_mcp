- Push-time CI (#10): install / typecheck / build on every push to main and
  on PRs, on ubuntu + macos, with strict `npm ci` lockfile install so a
  broken or platform-skewed `package-lock.json` fails loudly. Changelog
  entries now land as per-change fragment files in `changelog.d/`, folded
  into the version section at release time.
