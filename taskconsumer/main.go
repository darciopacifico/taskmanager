package main

import (
	"fmt"
	"os"
	"os/signal"
	"github.com/bsm/sarama-cluster"
	"github.com/darciopacifico/taskmanager/common"
	"github.com/op/go-logging"
	"github.com/darciopacifico/taskmanager/register"
	"github.com/darciopacifico/taskmanager/processor"
	"time"
	"github.com/Shopify/sarama"
)

var log = logging.MustGetLogger("tmg")

func init() {
	taskconsumer.RegisterTaskProcessor(processor.BirthdayProcessor{})
	taskconsumer.RegisterTaskProcessor(processor.InvoiceProcessor{})
	common.SetLogger()
}

func main() {
	log.Debug("Starting task consumer")

	config := cluster.NewConfig()
	//config.ClientID="taskManagerAgent"
	config.Consumer.Return.Errors = true

	config.Consumer.Offsets.Initial = sarama.OffsetNewest
	config.Consumer.Offsets.CommitInterval = time.Hour * 10

	// Specify brokers address. This is default one
	brokers := []string{"localhost:9092"}

	topic := "taskTopic"
	// Create new consumer
	master, err := cluster.NewConsumer(brokers, "taskConsumer", []string{topic}, config)
	if err != nil {
		panic(err)
	}

	defer func() {
		if err := master.Close(); err != nil {
			panic(err)
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
					log.Debug("Starting a goroutine for message consumption, msg id:", taskMessage.Id, " processor:", taskMessage.TaskProcessorName)
					//TODO: should be a processor pool, limiting concurrent processing?
					//TODO: disconnect from kafka when certain limit of messages enqued, connecting back when a safe limit was reached?
					//this behaviour could force messages to go other consumers
					//TODO: use a sync.WaitingGroup!!!

					go processMessage(taskProcessor, taskMessage)

				}else {
					taskMessage = common.TaskMessage{}
					taskMessage.Status = common.FAILED
					taskMessage.StatusMessage = fmt.Sprintf("Error trying to create taskProcessor! %v", err.Error())

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
	fmt.Println("Processed", msgCount, "messages")

}

//call safely a processTask function, under protection of defer + recover
func processMessage(taskProcessor common.TaskProcessor, taskMessage common.TaskMessage) {
	defer func() {
		//assure for not panicking out
		if r := recover(); r != nil {
			log.Error("Error trying to process message id ", taskMessage.Id, " processor ", taskMessage.TaskProcessorName)
			return
		}
	}()

	log.Debug("Starting message consumption, msg id:", taskMessage.Id, " processor:", taskMessage.TaskProcessorName)

	outputTask, errProc := taskProcessor.ProcessTask(taskMessage)
	produceTaskFeedback(outputTask, errProc)

}

//TODO: produce a feedback message
func produceTaskFeedback(task common.TaskMessage, err error) {
	if (err != nil) {
		log.Warning("Error trying to process message! send this back to some kafka topic!", err)
	}else {
		log.Debug("Message processed successfully!! Send this back to some kafka topic!")
	}
}



