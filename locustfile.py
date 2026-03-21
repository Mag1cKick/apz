from locust import HttpUser, task, between

class FacadeUser(HttpUser):
    wait_time = between(0.1, 0.5)

    @task(3)
    def post_message(self):
        self.client.post("/message", json={"msg": "performance_test"})

    @task(1)
    def get_messages(self):
        self.client.get("/messages")