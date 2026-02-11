## Testing

The test suite is built with **pytest** and supports both unit and integration tests.

### Installation
Install dependencies (including test dependencies):

```bash
poetry install --with test
```

### Setup

If you would like to use a local Kafka broker, set the environment variable `USE_LOCAL_KAFKA_BROKER_IN_TESTS` to 'yes', which will spin a broker using the testcontainers package. For faster/advanced iteration, you can spin up your own broker using the `local-plain-kafka-broker-docker-compose.yml` or `local-ssl-kafka-broker-docker-compose.yml`, and configure the address inside kafka_bootstrap fixture in conftest. </br> To use a remote broker, set the environment variable `USE_LOCAL_KAFKA_BROKER_IN_TESTS` to 'no'.`KAFKA_TEST_CLUSTER_BOOTSTRAP_SERVERS`, `KAFKA_TEST_CLUSTER_USERNAME` and `KAFKA_TEST_CLUSTER_PASSWORD` must be set as environnment variables.

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
