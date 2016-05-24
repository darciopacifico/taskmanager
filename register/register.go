package taskconsumer

import (
	"encoding/gob"
	"github.com/darciopacifico/taskmanager/common"
	"reflect"
	"errors"
	"bytes"
	"github.com/Shopify/sarama"
	"github.com/op/go-logging"
)

var log = logging.MustGetLogger("tmg")
var mapProcessor = make(map[string]reflect.Type)

func RegisterTaskProcessor(processor common.TaskProcessor) {
	//_ = gob.CommonType{}
	gob.Register(processor)
	name, rt := getNameType(processor)
	mapProcessor[name] = rt

	log.Debug("Registering TaskProcessor: ",name)
}


//name and type of
func getNameType(value interface{}) (string, reflect.Type) {
	rt := reflect.TypeOf(value)
	name := rt.String()

	star := ""
	if rt.Name() == "" {
		if pt := rt; pt.Kind() == reflect.Ptr {
			star = "*"
			rt = pt
		}
	}
	if rt.Name() != "" {
		if rt.PkgPath() == "" {
			name = star + rt.Name()
		} else {
			name = star + rt.PkgPath() + "." + rt.Name()
		}
	}

	return name, rt
}


//decode kafka message to pair of taskProcessor / taskMessage
func CreateTaskProcessor(msg *sarama.ConsumerMessage) (common.TaskProcessor, common.TaskMessage, error) {

	log.Debug("Decoding Kafka Message to a pair of TaskProcessor, TaskMessage")

	if(msg==nil || msg.Value==nil){
		log.Warning("Invalid message! Null (Fine if it's a shutdown!)")
		err := errors.New("Invalid Message! Null")
		taskMessage := common.TaskMessage{}
		taskMessage.Error = err
		taskMessage.StatusMessage = "Invalid Message! null"
		taskMessage.Status = common.FAILED
		taskMessage.Payload = nil
		return common.DummyProcessor{}, taskMessage, err
	}

	buffer := bytes.NewBuffer(msg.Value)
	decoder := gob.NewDecoder(buffer)

	taskMessage := common.TaskMessage{}

	err := decoder.Decode(&taskMessage)

	if (err != nil) {
		log.Error("Error trying to decode to TaskMessage", err)
		taskMessage.Error = err
		taskMessage.StatusMessage = "Error trying to decode to TaskMessage:" + err.Error()
		taskMessage.Status = common.FAILED
		taskMessage.Payload = nil
		return common.DummyProcessor{}, taskMessage, err
	}

	typeProc, hasType := mapProcessor[taskMessage.TaskProcessorName]

	if (!hasType) {
		log.Error("Error trying to create processor. Processor not registered!", taskMessage.TaskProcessorName)
		err := errors.New("Error trying to create processor. Processor not registered!")
		taskMessage.Error = err
		taskMessage.StatusMessage = "Error trying to create processor. Processor not registered!"
		taskMessage.Status = common.FAILED
		taskMessage.Payload = nil
		return common.DummyProcessor{}, taskMessage, err
	}

	processorValue := reflect.New(typeProc)

	processorInterface := processorValue.Elem().Interface()

	processor, isProcessor := processorInterface.(common.TaskProcessor)

	if (!isProcessor) {
		log.Error("Error trying to create processor. Registered type is not TaskProcessor compatible")
		err := errors.New("Error trying to create processor. Registered type is not TaskProcessor compatible")
		taskMessage.Error = err
		taskMessage.StatusMessage = "Error trying to create processor. Registered type is not TaskProcessor compatible"
		taskMessage.Status = common.FAILED
		taskMessage.Payload = nil
		return common.DummyProcessor{}, taskMessage, err
	}

	log.Debug("Processor of type ", taskMessage.TaskProcessorName, " successfully created!")
	return processor, taskMessage, nil
}

