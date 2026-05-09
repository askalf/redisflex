## Summary

<!-- 1-3 bullets on what this PR does and why. Skip the "what changed" auto-summary — the diff already shows that. Focus on the *why*. -->

## What's NOT in this PR

<!-- Anything reviewers might expect but doesn't belong here, with a one-line reason. Optional but useful. -->

## Test plan

- [ ] `npm run build` clean
- [ ] `npm run typecheck` clean
- [ ] `npm test` passes (factory + memory + queue)
- [ ] If touching ioredis-adapter: smoke-test against a real Redis (`docker run --rm -p 6379:6379 redis`)
- [ ] Live smoke (if applicable): describe what you ran and what you saw
