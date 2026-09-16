# Security Controls

Implemented controls include TLS CA verification, hostname verification, SCRAM support, service-specific credential inputs, redacted configuration, disabled topic auto-creation, authoritative producer/consumer safety settings, manual offset commit, and no credential logging in diagnostic output.

Infrastructure controls such as principal creation, ACL enforcement, certificate issuance and NetworkPolicy remain external evidence owned by the Kafka platform and cloud-services repositories.
