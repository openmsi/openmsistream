## Testing

The test suite is built with **pytest** and supports both unit and integration tests.

### Setup
Install dependencies (including test dependencies):

```bash
poetry install --with test
```

If you would like to use a local Kafka broker (instantiated with the testcontainers package), set the environment variable `USE_LOCAL_KAFKA_BROKER_IN_TESTS` to 'yes'; </br> Otherwise, you must set `KAFKA_TEST_CLUSTER_BOOTSTRAP_SERVERS`, `KAFKA_TEST_CLUSTER_USERNAME` and `KAFKA_TEST_CLUSTER_PASSWORD`.
### Run tests

Run the full test suite:

```
poetry run pytest
```

### Selective test execution

Tests can be filtered using markers:

```
poetry run pytest -m kafka
poetry run pytest -m "not kafka"
poetry run pytest -m "not encrypted"
```

### Kafka integration tests

Kafka-based tests use testcontainers for per-test, isolated clusters.
A Docker Compose–based Kafka setup is also available for faster local iteration (requires adjusting the kafka_bootstrap fixture).
