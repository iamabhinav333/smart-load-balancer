from __future__ import annotations

import os

from locust import HttpUser, between, task


TARGET_URL = os.getenv("LOAD_TEST_TARGET_URL", "http://127.0.0.1:7999")


class SmartLoadBalancerUser(HttpUser):
    host = TARGET_URL
    wait_time = between(0.1, 0.6)

    @task(5)
    def browse_data(self):
        self.client.get("/api/data", name="/api/data")

    @task(2)
    def inspect_stats(self):
        self.client.get("/stats", name="/stats")

    @task(2)
    def inspect_metrics(self):
        self.client.get("/api/metrics", name="/api/metrics")

    @task(1)
    def open_dashboard(self):
        self.client.get("/dashboard", name="/dashboard")