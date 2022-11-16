# What's in the project
## kafka-spring-example
This one contains a small Spring Boot REST service that demonstrates how to use Spring Kafka to:
* implement a declarative consumer using annotations
* implement a request-reply conversation using ReplyingKafkaTemplate

How to run:
* make sure you have Docker installed and running on your machine
* make sure you have Docker Compose installed on your machine
* execute `docker-compose up -d` from the "kafka-docker" directory
* run the ExamApiWebApplication (Spring Boot) using your IDE or CLI
* make the following HTTP request
  * POST http://localhost:8080/api/v1/exams/submit
  * {"name": "John", "lastName": "Doe", "regNumber": "123"}
* when done, run `docker-compose down`

## word-counter
This one contains a set of Spring components that demonstrate:
* how to implement a consumer and producer using the Kafka Clients library directly

How to run:
* run the IntegrationTest test class using your IDE or CLI

## tx-word-counter
This one contains a set of Spring components that demonstrate:
* how to implement a read-transform-write loop in a transactional fashion with Kafka

How to run:
* run the WordCounterTest test class using your IDE or CLI
