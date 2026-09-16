# Compliance Control Matrix

This document records engineering controls only and does not claim legal compliance or certification.

| Control objective | Implementation/evidence |
|---|---|
| Encryption in transit | Kafka TLS configuration enforced by SDK settings and platform |
| Authentication | SCRAM credentials supplied per service |
| Least privilege | ACL ownership remains Kafka-platform responsibility |
| Secret protection | Mounted secret files supported; redacted diagnostics |
| Change verification | deterministic tests + opt-in live round trip |
| Reliable processing | idempotent producer, manual commits, outbox/inbox patterns |
