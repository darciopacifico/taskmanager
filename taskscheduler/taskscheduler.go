package main

import (
	"bytes"
	"time"
	"flag"
	"os"
	"strings"
	"strconv"
	"io/ioutil"
	"encoding/gob"
	"os/signal"
	"encoding/json"
	"github.com/Shopify/sarama"
	"github.com/darciopacifico/taskmanager/common"
	"github.com/robfig/cron"
	"github.com/op/go-logging"
)

var (
	log = logging.MustGetLogger("tmg")

	KafkaBrokers string
	ScheduleFile string
	StrLogLevel string
	producer sarama.SyncProducer
)

func init() {
	parseFlags()
}

func main() {

	producer = creatProducer(KafkaBrokers) // externalize

	defer func() {
		if err := producer.Close(); err != nil {
			log.Error(err)
		}
	}()

	cron := cron.New()

	schedules := parseSchedules(ScheduleFile)

	for _, scheduleO := range schedules {

		scheduleCopy := deepCopy(scheduleO)

		log.Debug("Scheduling task ", scheduleCopy.Id, scheduleCopy.Cron, scheduleCopy.TaskMessageTemplate.TaskProcessorName)

		cron.AddFunc(scheduleCopy.Cron, func() {

			log.Debug("Producing scheduled job start request!", scheduleCopy.Cron, scheduleCopy.Id, scheduleCopy.TaskMessageTemplate.TaskProcessorName)

			msgCopy := deepCopyMsg(scheduleCopy.TaskMessageTemplate)

			msgCopy.Id = strconv.Itoa(int(time.Now().Unix())) //fake id
			msgCopy.Status = common.CREATED
			msgCopy.ScheduledAt = time.Now()
			sendMessage(msgCopy, scheduleCopy.Topic)

		})
	}

	cron.Start()

	waitForStopSignal()

	cron.Stop()

}


//maintain go routines running until stop signal
func waitForStopSignal(){
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)
	<-signals //
	log.Debug("Signal received! Stopping scheduler!!")
}

//serialize using gob and send message
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
		log.Debug("Message sent to partition", partition, "offset", offset)
		return nil
	}
}


//task message deepcopy
func deepCopy(taskSchedule common.TaskSchedule) common.TaskSchedule {
	buffer := bytes.NewBuffer([]byte{})
	encoder := json.NewEncoder(buffer)

	errE := encoder.Encode(taskSchedule)
	if (errE != nil) {
		log.Error("Error trying to generate a copy of template message for scheduling! E", errE)
		os.Exit(1)
	}

	reader := bytes.NewReader(buffer.Bytes())
	decoder := json.NewDecoder(reader)

	taskScheduleCopy := common.TaskSchedule{}

	errD := decoder.Decode(&taskScheduleCopy)
	if (errD != nil) {
		log.Error("Error trying to generate a copy of template message for scheduling! D", errD)
		os.Exit(1)
	}

	return taskScheduleCopy
}


//task message deepcopy
func deepCopyMsg(taskMsg common.TaskMessage) common.TaskMessage {
	buffer := bytes.NewBuffer([]byte{})
	encoder := json.NewEncoder(buffer)

	errE := encoder.Encode(taskMsg)
	if (errE != nil) {
		log.Error("Error trying to generate a copy of template message for scheduling! E", errE)
		os.Exit(1)
	}

	reader := bytes.NewReader(buffer.Bytes())
	decoder := json.NewDecoder(reader)

	taskMsgCopy := common.TaskMessage{}

	errD := decoder.Decode(&taskMsgCopy)
	if (errD != nil) {
		log.Error("Error trying to generate a copy of template message for scheduling! D", errD)
		os.Exit(1)
	}

	return taskMsgCopy
}


//load schedules
func parseSchedules(configFile string) []common.TaskSchedule {

	scheduleFilePath := ReadFile(configFile)
	taskSchedules := []common.TaskSchedule{}

	err := json.Unmarshal([]byte(scheduleFilePath), &taskSchedules)

	if (err != nil) {
		log.Error("Error trying to parse json schedule file!", err)
		os.Exit(1)
	}

	if (len(taskSchedules) == 0) {
		log.Warning("NO SCHEDULE INFO WAS FOUND! EXITING! file:", configFile)
		os.Exit(0)
	}

	return taskSchedules
}

//read all file content
func ReadFile(file string) string {
	content, err := ioutil.ReadFile(file)
	if err != nil {
		log.Error("Error trying to read schedule json file! See taskscheduler -help for more info!", err)
		os.Exit(1)
	}
	return string(content)
}


//parse all entry flags and configure application settings one time
func parseFlags() {
	if !flag.Parsed() {
		flag.StringVar(&ScheduleFile, "schedule", "", "JSON containing schedule spec elements.")
		flag.StringVar(&StrLogLevel, "l", "DEBUG", "Loglevel: DEBUG | INFO | NOTICE | WARNING | ERROR | CRITICAL")
		flag.StringVar(&KafkaBrokers, "brokers", "", "Comma separeted string containing the Kafka Brokers. Like: <host1>:9092,<host2>:9092")

		flag.Parse()

	}
}


//create a kafka message producer
func creatProducer(hosts string) sarama.SyncProducer {

	log.Debug("Creating a message producer:", hosts)
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll
	//config.Producer.Retry

	hosts = strings.TrimSpace(hosts)

	if (len(hosts) == 0) {
		log.Error("Please specify a kafka broker list. See taskscheduler -help for more info!")
	}

	arrHosts := strings.Split(hosts, ",")

	producer, err := sarama.NewSyncProducer(arrHosts, config)
	if err != nil {
		log.Error("Error trying to connect to Kafka b!", err)
		os.Exit(1)
	}

	log.Debug("Message producer successfully created!")
	return producer
}
