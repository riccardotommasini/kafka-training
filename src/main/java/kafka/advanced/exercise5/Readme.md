# Exercize 6: Word Count in Vanilla Kafka


## Before Starting

```bash
bin/kafka-topics --bootstrap-server localhost:9092 --create --topic paragraphs 

bin/kafka-topics --bootstrap-server localhost:9092 --create --topic words
```


# TODOS

Write a java application using Kafka producer/consumer APIs that performs wordcount

- populate a topic of sentences

- read the topic with a consumer

- split on space

- count the occurrence of each word 

Then try to restrict the count to the last 30 seconds

## References

[Useful library to generate the text (already in maven dependencies)](https://github.com/mdeanda/lorem)
