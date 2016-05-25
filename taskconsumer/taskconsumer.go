package main

import (
	"fmt"
	"os"
	"time"
	"flag"
	"strings"
	"sync"
	"os/signal"
	"github.com/Shopify/sarama"
	"github.com/bsm/sarama-cluster"
	"github.com/darciopacifico/taskmanager/common"
	"github.com/op/go-logging"
	"github.com/darciopacifico/taskmanager/register"
	"github.com/darciopacifico/taskmanager/processor"
)

var (
	log = logging.MustGetLogger("tmg")
	KafkaBrokers string
	StrLogLevel string
	TopicName string
	//avoid consumer abruptally stops
	waitingGroup = sync.WaitGroup{}
)

func init() {
	parseFlags()

	taskconsumer.RegisterTaskProcessor(processor.BirthdayProcessor{})
	taskconsumer.RegisterTaskProcessor(processor.InvoiceProcessor{})
	common.SetLogger(StrLogLevel)
}

//parse all entry flags and configure application settings one time
func parseFlags() {
	if !flag.Parsed() {
		flag.StringVar(&TopicName, "topic", "", "Kafka topic name for task scheduling!")
		flag.StringVar(&StrLogLevel, "l", "DEBUG", "Loglevel: DEBUG | INFO | NOTICE | WARNING | ERROR | CRITICAL")
		flag.StringVar(&KafkaBrokers, "brokers", "", "Comma separeted string containing the Kafka Brokers. Like: <host1>:9092,<host2>:9092")
		flag.Parse()
	}
}

func main() {

	config := cluster.NewConfig()
	config.Consumer.Offsets.Initial = sarama.OffsetNewest
	config.Consumer.Offsets.CommitInterval = time.Hour * 10

	KafkaBrokers = strings.TrimSpace(KafkaBrokers)

	if (len(KafkaBrokers) == 0) {
		log.Error("Please specify a kafka broker list. See taskconsumer -help for more info!")
		os.Exit(1)
	}

	arrHosts := strings.Split(KafkaBrokers, ",")

	// Create new consumer
	master, err := cluster.NewConsumer(arrHosts, "taskConsumer", []string{TopicName}, config)
	if err != nil {
		log.Error("Error trying to connect to kafka a! ", err)
		os.Exit(1)
	}

	defer func() {
		if err := master.Close(); err != nil {
			log.Error("Error when exiting Task Processor!", err)
			os.Exit(1)
		}
	}()

	// How to decide partition, is it fixed value...?

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	// Count how many message processed
	msgCount := 0


	// Get signnal for finish
	doneCh := make(chan struct{})
	go func() {
		for {
			select {
			case err := <-master.Errors():
				fmt.Println(err)
			case msg := <-master.Messages():
				msgCount++
				log.Debug("Message arrive!")

				taskProcessor, taskMessage, err := taskconsumer.CreateTaskProcessor(msg)

				if (err == nil) {
					log.Debug("Starting a goroutine for message consumption processor:", taskMessage.TaskProcessorName)
					//TODO: should be a processor pool, limiting concurrent processing?
					//TODO: disconnect from kafka when certain limit of messages enqued, connecting back when a safe limit was reached?
					//this behaviour could force messages to go other consumers
					//TODO: use a sync.WaitingGroup!!!

					waitingGroup.Add(1)
					go processMessage(taskProcessor, taskMessage)

				}else {
					taskMessage = common.TaskMessage{}
					taskMessage.Status = common.FAILED
					taskMessage.StatusMsg = fmt.Sprintf("Error trying to create taskProcessor! %v", err.Error())
					taskMessage.FinishedAt = time.Now()

					taskMessage.Error = err
					produceTaskFeedback(taskMessage, err)
				}

			case <-signals:
				fmt.Println("Interrupt is detected. Stop consumption!")
				doneCh <- struct{}{}
			}
		}
	}()

	<-doneCh

	log.Debug("Waiting for current running tasks...")
	waitingGroup.Wait()

	fmt.Println("Processed", msgCount, "messages")

}

//call safely a processTask function, under protection of defer + recover
func processMessage(taskProcessor common.TaskProcessor, taskMessage common.TaskMessage) {
	defer func() {
		//assure for not panicking out
		if r := recover(); r != nil {
			log.Error("Error trying to process message processor ", taskMessage.TaskProcessorName)
			return
		}

		waitingGroup.Done()
	}()

	log.Debug("Starting message consumption, msg processor:", taskMessage.TaskProcessorName)

	taskMessage.StartedAt = time.Now()
	outputTask, errProc := taskProcessor.ProcessTask(taskMessage)
	outputTask.FinishedAt = time.Now()
	produceTaskFeedback(outputTask, errProc)


}

//TODO: produce a feedback message
func produceTaskFeedback(task common.TaskMessage, err error) {

	duration := task.FinishedAt.Sub(task.StartedAt)

	log.Debug("Task processor ", task.TaskProcessorName, " takes ", duration)

	if (err != nil) {
		log.Warning("Error trying to process message! send this back to some kafka topic!", err)
	}else {
		log.Debug("Message processed successfully!! Send this back to some kafka topic!", task.Id)
	}
}