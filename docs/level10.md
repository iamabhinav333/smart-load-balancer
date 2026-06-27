# Level 10 - Advanced Distributed Systems Features

## What Changed

- Sticky sessions route the same session identifier to the same backend.
- Redis-backed shared state is supported for session bindings and backend health.
- WebSocket traffic is proxied end-to-end through the balancer.
- A local autoscaler can spawn additional backend replicas when load rises.
- Service discovery updates the balancer when new backend services appear.

## Runtime Notes

Set `BALANCER_REDIS_URL` to enable Redis. If it is not set, the system falls back to the local JSON state store in `ha_state/`.
For sticky sessions, send a `slb_session` cookie or `X-Session-Id` header.

Suggested load test flow:

```bash
locust -f locustfile.py --host http://127.0.0.1:8000
```

Suggested HA demo flow:

```bash
python -m scripts.run_ha_demo
```

WebSocket endpoint:

```text
ws://127.0.0.1:8000/ws
```
