# taskmanager

## Task manager based on github.com/robfig/cron and Cluster/Sarama.

The task manager solution was designed in 3 parts:

- taskscheduler:
Go application that parse the JSON containing the task schedule information, register the task in a cron like mechanism. When cron triggers an scheduled event, the taskscheduler produces a task start signal, sending a message task to Kafka/Zookeeper topic. Taskmanager architectures can have any number of taskschedulers, managing different schedule tasks.

- message broker: 
Kafka/Zookeeper cluster that holds a partitioned topic to receive and serve TaskMessage stream. Important! Topic must be partitioned at minimum the same number of potential consumers. In production environment the topic replication-factor > 1 can assure for message delivery.

- taskconsumer:
Go application that listen and consumes task messages from Kafka. This go app instances could be freely instantiated and gracefully stopped (Ctrl+C Signal, SIGINT etc) at any time. The signal to interrupt will make taskconsumer to stop to receive new messages and wait for current message consumption.

When a new taskconsumer app is instantiated, the Zookeeper imediatelly rebalance the message delivery to all instances, same when one taskconsumer is stopped.


