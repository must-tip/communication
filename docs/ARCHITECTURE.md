# Architecture

## Ownership boundaries

- `kafka_platform_service`: KRaft brokers/controllers, topics, TLS, SCRAM principals, ACLs, retention and replication.
- `cloud-services`: workload capability labels, Kafka DNS, CA/credential mounts and environment injection.
- `musttip-communication`: client validation, EventBus, producer/consumer safety, envelopes, signing, outbox/inbox and diagnostics.
- Microservice: business topics/events, handler registration, business transactions and service-specific settings.

## Runtime path

```text
Kafka platform Secret/CA -> cloud-services workload -> Django settings -> CommunicationSettings -> KafkaTransport -> confluent-kafka -> Kafka
```

No application may auto-create topics or use the admin principal. Public imports remain under `communication.*`.
