# Security

Report vulnerabilities privately through
[GitHub security advisories](https://github.com/JohanLindvall/proxytron/security/advisories/new).
Include the affected version, a minimal synthetic reproducer and the expected
behavior. Do not include real credentials, proxy inventories or third-party targets.

Fixes target the latest release. Do not rely on public proxies for confidentiality:
keep destination TLS verification enabled, and protect persistence files because
they contain complete endpoint credentials. Egress IP checks measure an address;
they do not certify a proxy's anonymity or trustworthiness.
