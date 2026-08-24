- Run-as-main detection canonicalises the entry module path as well as
  `argv[1]`, so launching through a symlinked directory (macOS `/var` →
  `/private/var`, `--preserve-symlinks`) still boots the server.
