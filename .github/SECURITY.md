# Security Policy

## Reporting

Do not post credentials, API keys, tokens, private keys, account identifiers, or
exchange secrets in public issues, pull requests, logs, screenshots, or patches.

For security-sensitive reports, use GitHub's private security advisory flow when
available for this repository, or contact the repository owner privately.

## Credential exposure

If a Bybit, LLM-provider, GitHub, or other credential is ever committed or shown
publicly, treat it as compromised immediately:

1. Revoke or rotate the credential at the provider.
2. Remove it from the current tree.
3. Audit git history, workflow logs, artifacts, issues, and pull requests for copies.
4. Do not rely on deleting a file or rewriting only the latest commit.

Runtime secrets belong in local environment variables or an ignored `.env` file.
The tracked `.env.example` must contain placeholders only.

## Supported code

Security fixes should target the default branch and the currently active runtime
branch before release or deployment.
