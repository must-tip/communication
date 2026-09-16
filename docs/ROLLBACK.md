# Rollback

Rollback the SDK package version without renaming topics, consumer groups, event IDs or database inbox/outbox tables. Do not reset Kafka offsets or delete outbox/inbox rows as a normal application rollback. Preserve the existing Kubernetes Secrets and CA mounts. Validate the rollback with the same live diagnostic before restoring normal traffic.
