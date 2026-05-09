# Security policy

## Supported versions

redisflex is in early development. Only the latest minor version receives security updates.

| Version | Supported |
|---------|-----------|
| latest  | Yes       |
| <latest | No        |

## Reporting a vulnerability

If you find a security issue, please **do not open a public GitHub issue**. Instead, email `security@askalf.org` (or open a private security advisory via GitHub's "Report a vulnerability" button on the [Security tab](https://github.com/askalf/redisflex/security/advisories)) with:

- A description of the issue
- Steps to reproduce
- The version of redisflex you tested against
- Your assessment of impact

You can expect:

- An acknowledgement within 48 hours
- An assessment within 1 week
- Credit in the eventual fix's release notes if you'd like (let us know)

## Scope

In scope:

- Glob-pattern injection in `keys()` / `psubscribe()` that lets an untrusted input match keys or channels it shouldn't.
- Memory blowup in any of the in-memory backends triggered by adversarial input (oversized values, pathological patterns, etc.).
- Anything that lets a downstream user bypass `maxRetriesPerRequest` defaults in ioredis mode.
- Anything that crashes the adapter in a way an untrusted-input client could trigger remotely.

Out of scope:

- Vulnerabilities in `ioredis` itself — please report those upstream.
- Memory-mode persistence: it's intentional that memory mode loses everything on restart.
- DoS via large value sizes — bound by your application's memory budget.
- The `eval()` fallback returning `0` for unrecognized scripts: this is documented behavior, not a security boundary.
