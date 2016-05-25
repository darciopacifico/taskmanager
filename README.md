# taskmanager

Go Task manager based on github.com/robfig/cron, Cluster/Sarama and Kafka.

# The task manager solution was designed in 3 parts:

* taskscheduler

Go application that parse the JSON containing the task schedule specs and register schedule in a cron like mechanism. When the cron triggers an scheduled event, taskscheduler produces a task start signal, sending a message task to Kafka topic. Taskmanager architecture allows any number of taskschedulers instances, managing different schedule tasks.

* message broker

Kafka cluster that holds a partitioned topic to receive and serve TaskMessage stream. Important! Topics must be partitioned at minimum the same number of potential consumers. In production environment the topic replication-factor > 1 can assure for message delivery and availability of kafka message service.

* taskconsumer

Go application that listen and consumes task messages from Kafka. This go app instances could be freely instantiated and gracefully stopped (Ctrl+C Signal, SIGINT etc) at any time. The signal to interrupt will make taskconsumer to stop to receive new messages and wait for current message consumption.

When a new taskconsumer app is instantiated, the Zookeeper imediatelly rebalance the message delivery to all instances, same when one taskconsumer is stopped.

## How to start the solution

* Requirements: Java 7, Go SDK
- Download and descompact [kafka_2.11-0.9.0.1](https://www.apache.org/dyn/closer.cgi?path=/kafka/0.10.0.0/kafka_2.11-0.10.0.0.tgz)
- Start zookeeper: [kafka_dir/bin]/zookeeper-server-start.sh ../config/zookeeper.properties
- Start kafka: <kafka_dir/bin>/kafka-server-start.sh ../config/server.properties
- Create sample topic: <kafka_dir/bin>/kafka-topics.sh --create --topic taskTopic --zookeeper localhost:2181 --partitions 12 --replication-factor 1




