# Levels 8 and 9

## Level 8 - Load Testing

Use Locust to drive traffic against the HA gateway or a single balancer.

```bash
locust -f locustfile.py --host http://127.0.0.1:7999
```

Suggested experiments:
- Start with 50 to 100 users to confirm the app stays stable.
- Increase to several hundred users to measure latency and throughput.
- Watch `/api/metrics` for backend request share, failures, and cache behavior.

## Level 9 - High Availability

Run two balancer instances and a gateway that routes to the healthy one.

Example launch configuration:
- LB1: `BALANCER_INSTANCE_ID=lb1 BALANCER_PORT=8100 BALANCER_PRIORITY=1`
- LB2: `BALANCER_INSTANCE_ID=lb2 BALANCER_PORT=8101 BALANCER_PRIORITY=2`
- Gateway: `HA_GATEWAY_PORT=8099`

Shared state lives in `ha_state/` by default.

Verify failover by:
1. Open the gateway at `http://127.0.0.1:8099`.
2. Stop the primary balancer.
3. Confirm the gateway continues returning responses from the secondary balancer.
4. Check `http://127.0.0.1:8099/api/cluster` for live instance status.