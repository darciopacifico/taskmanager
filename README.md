# taskmanager

Go Task manager based on github.com/robfig/cron, Cluster/Sarama and Kafka.

First, I would like to apologize for, not having been able to implement unit tests for this solution.

**The task manager solution was designed in 3 parts:**

* taskscheduler (Available on Docker as darcio/dlp_taskscheduler)

Go application that parse the [JSON](https://github.com/darciopacifico/taskmanager/blob/master/config/taskscheduler.json) containing the task schedule specs and register the schedules in a cron like mechanism. When cron triggers an scheduled event, taskscheduler produces a kafka message task to topic *taskTopic*. This message is a signal to task starting.

Taskmanager architecture allows any number of taskschedulers instances, managing different schedule tasks files, sending messagens to different kafka topics. As a TODO would be great to taskscheduler to receive step-by-step processing confirmation from consumer, generate metrics, implement retry policy, priorization etc.

* message broker (Available on Docker as darcio/dlp_kafka)

Kafka cluster that manages message topics. Important! Topics must be partitioned at minimum the same number of potential consumers. In production environment the topic replication-factor > 1 can assure for message delivery and availability of kafka message service.

Kafka is the component that allows the entire solution to be dynamicaly scalable as requested, actualy.

* taskconsumer (Available on Docker as darcio/dlp_taskconsumer)

Go application that listen and consumes task messages from Kafka. This go app instances can be freely instantiated and gracefully stopped (Ctrl+C Signal, SIGINT etc) at any time, **in multiple instances in same or different cluster nodes**. The signal to interrupt will make taskconsumer to stop message receiving and wait for current message consumption.

This app commit the topic/partition offset at every message, before message processing. There is no current policy for retry, DLQ etc.

As consumer example, there is an Invoice Processor and a Birthday Greatings Processor apps. Both sample apps only print log messages.

When a new taskconsumer app is instantiated or stopped, Kafka + Cluster/Sarama lib immediately rebalance the message delivery to all remaining and/or new instances with same groupid, no matter how many instances or location.

If no consumer is available, all messages stay stored in kafka until one or more consumers instantiation.

## How to start the solution

* Requirements: Docker or Java 7, Go SDK

### Docker compose installation

- Download docker-compose.yml in an empty directory
- Using terminal go the directory containing the docker-compose.yml
- execute [docker-compose](https://raw.githubusercontent.com/darciopacifico/taskmanager/master/docker-compose.yml) up

### Manual kafka and Apps installation 

- Download and descompact [kafka_2.11-0.9.0.1](https://www.apache.org/dyn/closer.cgi?path=/kafka/0.10.0.0/kafka_2.11-0.10.0.0.tgz)
- Start zookeeper: [kafka_dir/bin]/zookeeper-server-start.sh ../config/zookeeper.properties
- Start kafka: [kafka_dir/bin]/kafka-server-start.sh ../config/server.properties
- Create sample topic: [kafka_dir/bin]/kafka-topics.sh --create --topic taskTopic --zookeeper localhost:2181 --partitions 12 --replication-factor 1


#### app compiling

* Clone and compile taskmanager app

  git clone https://github.com/darciopacifico/taskmanager.git

  cd $GOPATH/src/github.com/darciopacifico/taskmanager/

  go get ./...

  go install ./...

#### app running

  - start taskscheduler, Terminal 1

    taskscheduler -brokers=localhost:9092 -schedule=config/taskscheduler.json
    
  - start tastconsumer, multiple instances, Terminals 2 to N 

    taskconsumer -l=DEBUG -topic=taskTopic -brokers=localhost:9092

  


