# Security policy

Kontra holds other people's money in escrow and brokers jobs between parties who do not trust
each other, so we take reports seriously and fix them first. The security model is in
[docs/security.md](docs/security.md).

## Reporting a vulnerability

**Report privately, through GitHub:** open
[a private vulnerability report](https://github.com/medmahmoudi26/kontra-sdk/security/advisories/new)
(the repository's **Security** tab, then **Report a vulnerability**). Only the maintainers see
it, and we can work on the fix and the advisory with you there.

Please do not open a public issue, a pull request, a discussion, or a file under `.scratch/`
for a vulnerability before it is fixed.

A useful report says:

- what an attacker can do, and in which role (seller, buyer, an unauthenticated network
  peer, someone who obtained Kontra's stored data);
- the affected component and version or commit;
- how to reproduce it, ideally against `kontra dev`, `kontra.testing.Marketplace` or the e2e
  stack (`make stack-up`);
- anything you know about a fix.

## Scope

In scope, in this repository:

- the `kontra` package (`kontra-sdk` on PyPI): the contract, codec, artifacts, gate client,
  `kontra serve`, `kontra.hire`, `kontra mcp`, the CLI, and `kontra.testing` where a fake would
  hide a real flaw;
- `temporal/authorizer`: the Authorizer, the callback guard, the server binary and the
  `ghcr.io/medmahmoudi26/kontra-temporal` image;
- `kontra.devgate` and `kontra dev` (the dev stack's loopback exposure, decisions I34),
  `compose.yaml` (the e2e stack), `observability/compose.yaml` (`kontra dev up
  --observability`), the examples, and the CI workflows and release process.

The hosted platform (the gate, the console) lives in a private repository; report problems
with it here as well and we will route them.

Out of scope: findings that need a compromised party's own machine or key (that party's jobs
are theirs to lose; the question is whether it reaches anyone else's), denial of service by
volume, and the `kontra` package on PyPI, which is an unrelated project (ours is
`kontra-sdk`).

An issue in Temporal itself belongs with Temporal's security team
([temporal.io/security](https://temporal.io/security/), `security@temporal.io`). If it affects
Kontra, tell us too: `temporal/authorizer/callbackguard` exists because of one.

## Testing

Use `kontra dev` or `kontra.testing.Marketplace` (in-process) for the gate's API and escrow,
and the Authorizer through `make deny-matrix` or the e2e stack (`make e2e`: `kontra dev`'s
Temporal dev server has no Authorizer), rather than the hosted service. Against the hosted service: test only with accounts and data you own,
do not read, change or delay other parties' jobs or funds, and stop and report as soon as you
can show impact. We will not pursue good-faith research that follows these rules.

## What happens next

We aim to acknowledge a report within three working days, agree on the severity and a
disclosure date with you, and publish a GitHub security advisory with the fix, crediting you
unless you prefer otherwise. Kontra is pre-1.0: fixes land on `dev` and ship in the next
release of `kontra-sdk`, and in the Temporal image that `dev` pushes
(`ghcr.io/medmahmoudi26/kontra-temporal:dev` and its commit tag); older releases are not
patched.
