# Testing

## Layer 1: deterministic SDK tests

`pytest -q` validates configuration normalization, security invariants, producer/consumer behavior, outbox/inbox logic and diagnostics without a broker.

## Layer 2: standalone live transport test

Run `musttip-communication-kafka-check --source environment` inside a Kafka-enabled pod. This isolates the SDK plus Kafka/network/credentials from Django settings.

## Layer 3: microservice integration test

Run the same command with `--source django`. This uses `DJANGO_SETTINGS_MODULE` and proves the microservice converts the injected server contract into the SDK contract.

## Layer 4: pytest live round trip

Set `RUN_KAFKA_INTEGRATION=1` and `COMMUNICATION_KAFKA_TEST_TOPIC`, then run `pytest -m integration tests/test_kafka_live.py`.

A dedicated pre-provisioned topic and ACL-authorized consumer group prefix are required. Never use production business topics for diagnostics.
