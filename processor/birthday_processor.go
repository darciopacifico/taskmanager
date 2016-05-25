package processor

import (
	"github.com/darciopacifico/taskmanager/common"
	"github.com/op/go-logging"
	"time"
)

var log = logging.MustGetLogger("tmg")

type BirthdayProcessor struct{}


//process birthday messages
func (i BirthdayProcessor) ProcessTask(input common.TaskMessage) (common.TaskMessage, error) {

	log.Debug("Processing birthdays congrats!!!")

	time.Sleep(200 *time.Millisecond)

	input.Status = common.FINISHED
	return input, nil

}

