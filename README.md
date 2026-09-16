# Communication service contract

This package is the authentication service's transport-neutral event layer. It
preserves the existing topic/feed names and public functions while using the
Kafka platform's security and reliability contracts.

## Kafka platform integration

Production clients use the internal bootstrap service:

```text
kafka-bootstrap.kafka-system.svc.cluster.local:9092
```

Required workload values are `KAFKA_BOOTSTRAP_SERVERS`, a stable
`KAFKA_CLIENT_ID`, a service-specific `KAFKA_USERNAME` and `KAFKA_PASSWORD`, and
`KAFKA_CA_FILE=/var/run/secrets/kafka/ca.crt`. Secrets must come from the
existing Vault/Kubernetes Secret integration and must never use the Kafka
platform's `admin` principal.

The client enforces:

- SASL/SCRAM-SHA-512 over TLS when `SASL_SSL` is selected;
- `acks=all`, idempotent production, bounded in-flight requests, and zstd by
  default;
- manual offset commits only after the handler and idempotency record succeed;
- `read_committed` isolation and disabled automatic topic creation;
- stable consumer group and client identities supplied by the workload;
- bounded timeouts, retries, payload size, headers, and graceful shutdown.

The package accepts both the nested `COMMUNICATION["KAFKA"]` format and the
Kafka platform's dotted `KAFKA`/`KAFKA_CONSUMER` dictionaries. Explicit
communication values take precedence. Producer-only options inherited through
`KAFKA_CONSUMER = {**KAFKA, ...}` are removed before constructing the consumer.

## Topics, ACLs, and feed compatibility

Application topics and their `.dlq` topics must be provisioned through the
Kafka platform topic workflow before deployment. The communication client never
creates them. Existing feed strings are passed through the existing
`canonical_topic` policy; this change does not rename any configured feed.

Create a dedicated authentication-service principal and least-privilege ACLs:

- `Write`, `Describe`, and idempotent-write permissions for produced topics;
- `Read` and `Describe` for consumed topics;
- `Read`/`Describe` for only the service's consumer-group prefix;
- `Write`/`Describe` for the corresponding DLQ topics.

Do not grant cluster-admin or wildcard application permissions. Topic
replication, minimum ISR, retention, and cleanup policy remain owned by the
Kafka platform repository and its audited reconciliation workflow.

## Delivery and transaction model

Direct publication is at least once. Use Django `transaction.on_commit()` when
a best-effort event must not be emitted before a database transaction commits.
For guaranteed database/event atomicity, call `OutboxService.enqueue()` or the
compatibility-named `enqueue_on_commit()` *inside* the business transaction.
Both now insert the outbox row immediately so it commits or rolls back with the
business change. Run `drain_event_outbox --loop` or the Celery task as a
separate worker.

Consumers run as separate deployments or management commands, never inside web
workers. Handlers must be idempotent. Database-backed inbox idempotency is the
recommended production backend, especially when Kafka and NATS consume the
same logical events. Poison events are published to the existing feed plus
`.dlq`; only the exception type and routing metadata are added, not exception
text or secrets.

## Operations and observability

Use `await EventBus.health()` for redacted transport readiness and broker
connection diagnostics. Aggregate structured logs and alert on:

- publish failures and undelivered messages during shutdown;
- consumer worker termination and offset commit/seek failures;
- idempotency storage errors;
- DLQ publication or sustained DLQ volume;
- outbox age, retry count, failed rows, and lease expiration;
- consumer lag from the platform's selected authenticated lag exporter.

Readiness should fail when a required transport is unavailable. Liveness should
only prove the process can make progress; it must not restart a healthy process
for a transient broker failure. Retain correlation, trace, event, and consumer
group identifiers in logs, but never payloads, credentials, signatures, or
tokens.

## Rollout, recovery, and testing

1. Provision existing topics/DLQs and least-privilege ACLs.
2. Mount the platform CA and inject the service principal from Vault.
3. Run Django checks and the communication unit suite.
4. Deploy an outbox worker and one consumer replica, then verify publish,
   consume, commit, redelivery, and DLQ behavior.
5. Scale consumers no higher than useful partition parallelism.
6. Roll out gradually and monitor failures, outbox age, and lag.

Rollback preserves topic names, consumer group IDs, inbox/outbox tables, and
offsets. Never delete inbox/outbox rows or reset offsets as an application
rollback step. Disaster recovery follows the Kafka platform runbook and must
verify synchronized group offsets before consumers resume in the recovery
cluster.
