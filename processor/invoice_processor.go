package processor

import (
	"github.com/darciopacifico/taskmanager/common"
)

type InvoiceProcessor struct{}


//consumes task message. Search for all billing info and generate invoices to all customers
func (i InvoiceProcessor) ProcessTask(input common.TaskMessage) (common.TaskMessage, error) {

	log.Debug("Starting Invoice Processing!")
	log.Debug("Starting Invoice Processing finished!")

	input.Status = common.FINISHED
	return input, nil

}
