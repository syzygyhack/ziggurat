# Security Policy

Ziggurat is a networked daemon: nodes discover each other over gossip, accept
tasks over an HTTP API, and exchange data and work over gRPC. We take security
reports seriously and appreciate responsible disclosure.

## Supported Versions

Ziggurat is pre-1.0. Security fixes are applied to the latest released minor
version only. Always run the most recent release.

| Version | Supported |
|---------|-----------|
| latest `0.x` release | ✅ |
| older releases | ❌ |

## Reporting a Vulnerability

**Please do not open a public issue for security vulnerabilities.**

Report privately via GitHub's private vulnerability reporting:

1. Go to the repository's **Security** tab.
2. Click **Report a vulnerability**.
3. Provide a description, affected version/commit, reproduction steps, and the
   impact you observed.

We aim to acknowledge a report within 3 business days and to provide an initial
assessment within 7 days. Once a fix is available we will coordinate a release
and credit you in the advisory unless you prefer to remain anonymous.

## Scope

Relevant areas include, but are not limited to:

- The HTTP API and its authentication (bearer token), including authorization
  bypass, request smuggling, or resource exhaustion.
- Cluster join and node enrollment (join tokens, mTLS CSR signing).
- The gossip and gRPC transports between nodes.
- The content-addressed store and FUSE mount (path traversal, unauthorized
  read/write).
- Task execution and workspace isolation.

## Out of Scope

- Issues that require a pre-compromised host or physical access.
- Running Ziggurat with security features disabled on an untrusted network.
  Ziggurat's zero-config defaults target a **trusted LAN**; exposing a node to
  an untrusted network without TLS, join tokens, and API authentication enabled
  is a deployment misconfiguration, not a vulnerability.
- Denial of service from unbounded task submission by an already-authenticated
  client.

## Hardening Guidance

For deployments beyond a trusted LAN, enable:

- mTLS between nodes (`security.tls.enabled=true`) and a cluster join token.
- API bearer-token authentication (`security.api_token`). The CLI sends the token
  via the `--token` flag, the `ZIGGURAT_TOKEN` env var, or `security.api_token`
  in its config.
- A firewall restricting the gossip, gRPC, and HTTP ports to known peers.

See the README's security and cross-platform sections for details.
