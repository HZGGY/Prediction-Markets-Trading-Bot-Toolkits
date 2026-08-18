# Local safety patch

This directory is a minimal source copy of the official
`polymarket_client_sdk_v2` crate version `0.6.0`.

- Upstream repository: <https://github.com/Polymarket/rs-clob-client-v2>
- Published crate SHA-256:
  `e0ed9ca91088808232ee7e6b48f8f4eaa846882f799c40f8190b0a0e3d28ed5e`
- License: MIT; the upstream `LICENSE` file is retained.

Only three upstream source files differ from the published crate:

1. `src/clob/client.rs` sets reqwest's redirect policy to `Policy::none()`.
   A signed request can therefore receive a redirect response, but reqwest
   cannot replay it to the redirect target.
2. `src/lib.rs` preserves the real successful HTTP status when a required
   JSON response is `null`, instead of synthesizing HTTP 404.
3. `src/error.rs` exposes that condition as `EmptyResponse`, containing only
   the real status, method, and path. It never retains or renders the body or
   authentication headers.

The application adapter treats a redirect response and `EmptyResponse` as an
uncertain order result. It does not retry, and the existing execution circuit
breaker persists a halt before another live order can be submitted.

When an official release fixes both behaviors, compare that release against
these three changes, run the complete offline safety suite, and remove the
`[patch.crates-io]` entry rather than carrying this local patch indefinitely.
