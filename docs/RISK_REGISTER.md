# Risk Register

| Risk | Status | Mitigation |
|---|---|---|
| Ambiguous Kafka publish timeout | Controlled | `PublishOutcomeUnknown`, stable event IDs, durable outbox/idempotent consumers |
| Consumer handler exceeds poll interval | Controlled | Config validation requires 30s minimum margin |
| Consumer worker fatal exit | Detectable | Transport health records worker failure; process monitoring/readiness required |
| Wrong service ACL/topic | Detectable | Dedicated live round-trip diagnostic |
| Credential/env naming drift | Controlled | Platform aliases and Secret-file inputs normalized centrally |
| Diagnostic affects business consumers | Controlled | Dedicated diagnostic topic is mandatory |
