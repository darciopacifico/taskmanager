package main

import (
	"github.com/Shopify/sarama"
	"github.com/darciopacifico/taskmanager/common"
	"encoding/gob"
	"bytes"
	"github.com/robfig/cron"
	"github.com/op/go-logging"
	"time"
)

var (
	producer = creatProducer([]string{"localhost:9092"}) // externalize
	log = logging.MustGetLogger("tmg")
)

func main() {

	defer func() {
		if err := producer.Close(); err != nil {
			log.Error(err)
		}
	}()

	cron := cron.New()

	cron.AddFunc("* * * * * *", func() {
		sendMessage(common.TaskMessage{
			Id            :"1",
			Payload       :"zubalele",
			Status        :common.CREATED,
			TaskProcessorName : "github.com/darciopacifico/taskmanager/processor.InvoiceProcessor",
			StatusMsg :"",
			Error              :nil,
			TaskMetadata  :map[string]string{},
			ScheduledAt : time.Now(),
		}, "taskTopic")
	})

	cron.Start()

	time.Sleep(1 * time.Hour)

	log.Debug("finished")

}


//create a kafka message producer
func creatProducer(hosts []string) sarama.SyncProducer {

	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll
	//config.Producer.Retry


	producer, err := sarama.NewSyncProducer(hosts, config)
	if err != nil {
		log.Error(err)
	}

	return producer
}

func sendMessage(taskMsg common.TaskMessage, topicName string) error {

	buffer := bytes.NewBuffer([]byte{})
	encoder := gob.NewEncoder(buffer)
	encoder.Encode(taskMsg)

	msg := &sarama.ProducerMessage{Topic: topicName, Value:sarama.ByteEncoder(buffer.Bytes())}

	partition, offset, err := producer.SendMessage(msg)
	if err != nil {
		log.Error("FAILED to send message: %s\n", err)
		return err
	} else {
		log.Debug("> message sent to partition %d at offset %d\n", partition, offset)
		return nil
	}
}

