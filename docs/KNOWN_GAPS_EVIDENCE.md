# Known Gaps and Evidence

- A real Kafka round-trip was not executed in this build environment because it has no route/credentials to the project's Kafka cluster. The opt-in live test and CLI diagnostic are included for execution inside the target environment.
- Kafka platform certificate/ACL/NetworkPolicy correctness is external to this SDK and must be evidenced in their owning repositories/cluster.
- Consumer worker failure is exposed in transport health; the deployment must still route readiness/alerts correctly.
