package common

import (
	"github.com/op/go-logging"
	"os"
	"time"
)

type TaskSchedule struct {
	Id                  string      `json:"Id"`
	Cron                string      `json:"Cron"`
	Topic               string      `json:"Topic"`
	TaskMessageTemplate TaskMessage `json:"TaskMessageTemplate"`
}

//Wrapper for a task message
type TaskMessage struct {
	Id 								string 							`json:"-"`
	TaskProcessorName string              `json:"TaskProcessorName"`
	TaskParam         string   						`json:"TaskParam"`
	Status            TaskStatus          `json:"-"`
	StatusMsg         string              `json:"-"`
	Error             error               `json:"-"`
	ScheduledAt       time.Time           `json:"-"`
	StartedAt         time.Time           `json:"-"`
	FinishedAt        time.Time           `json:"-"`
}

//basic task processor contract
type TaskProcessor interface {
	ProcessTask(input TaskMessage) (TaskMessage, error)
}

type DummyProcessor struct{}

func (d DummyProcessor) ProcessTask(input TaskMessage) (TaskMessage, error) {
	return input, nil
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
func SetLogger(logLevel string) {

	format := logging.MustStringFormatter("%{color}%{time:15:04:05.000} PID:%{pid} %{shortfunc} ▶ %{level:.4s} %{id:03x}%{color:reset} %{message}")
	backend1 := logging.NewLogBackend(os.Stdout, "", 0)
	backend1Formatter := logging.NewBackendFormatter(backend1, format)
	logging.SetBackend(backend1Formatter)

	level, _ := logging.LogLevel(logLevel)

	logging.SetLevel(level, "tmg")

}

