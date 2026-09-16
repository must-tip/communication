# Operations Runbook

1. Confirm the workload has `must-tip.com/kafka-client=true`.
2. Confirm `/var/run/secrets/kafka/ca.crt`, username and password mounts exist.
3. Run the environment diagnostic.
4. If it fails at `configuration`, fix mounts/env names.
5. If it fails at `broker_metadata`, investigate DNS, NetworkPolicy, TLS, SCRAM or broker health.
6. If it fails at `subscription`, investigate group/topic ACLs and broker availability.
7. If it fails at `publish_ack`, investigate topic Write/Describe ACLs and ISR/broker health.
8. If it fails at `receive`, investigate topic Read/group ACLs, rebalances and consumer processing.
9. If environment mode succeeds but Django mode fails, fix the microservice settings mapping.
10. Do not expose secrets or use the admin principal for diagnosis.
