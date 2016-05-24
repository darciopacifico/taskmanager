package common

import (
	"github.com/op/go-logging"
	"os"
)

type TaskSchedule struct {

}

//Wrapper for a task message
type TaskMessage struct {
	Id            string
	Payload       interface{}
	Status        TaskStatus
	TaskProcessorName string
	StatusMessage string
	Error	      error
	TaskMetadata  map[string]string
}

//basic task processor contract
type TaskProcessor interface{
	ProcessTask(input TaskMessage) (TaskMessage, error)
}

type DummyProcessor struct {}
func (d DummyProcessor) ProcessTask(input TaskMessage) (TaskMessage, error){
	return input,nil
}

//constant for task status
type TaskStatus int
const (
	CREATED TaskStatus = iota
	SCHEDULED
	EXECUTING
	FAILED
	FINISHED
)

//config a logger to specified module
func SetLogger() {

	format := logging.MustStringFormatter("%{color}%{time:15:04:05.000} PID:%{pid} %{shortfunc} ▶ %{level:.4s} %{id:03x}%{color:reset} %{message}")
	backend1 := logging.NewLogBackend(os.Stdout, "", 0)
	backend1Formatter := logging.NewBackendFormatter(backend1, format)
	logging.SetBackend(backend1Formatter)

	logging.SetLevel(logging.DEBUG, "tmg")

}

