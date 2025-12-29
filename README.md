# Sample project to highlight issue after startup

Metrics are working as expected, however after starting the application a warning is logged:
```
This Gauge has been already registered 
```

## Issue

* After starting/deploying a kafka-app, metrics seem to be registered multiple times
```
This Gauge has been already registered (MeterId{name='kafka.consumer.fetch.size.avg', tags=[tag(client.id=local-itunes-listener-0),tag(topic=first)]}), the registration will be ignored. Note that subsequent logs will be logged at debug level.
```

## Expected behavior

* Registration happens only once
* No warnings are logged

## Reproducing the Issue

* Run `docker compose up -d` to start Kafka
* Compile the demo application: `./mvnw clean verify -DskipTests`
* Run the app: `java -jar target/spring-kafka-micrometer-registration-issue-0.0.1-SNAPSHOT.jar`
* Issue is only observed after startup
* Message varies (different metrics are reported)


Related issue: https://github.com/micrometer-metrics/micrometer/issues/5757

