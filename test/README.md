## Testing

The test suite is built with **pytest** and supports both unit and integration tests.

### Setup
Install dependencies (including test dependencies):

```bash
poetry install --with test
```

You must 
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
