package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/google/uuid"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
)

// Scenario:
// * Workflow has simple update handler and starts activity
// * Workflow worker stops after activity started
// * Update sent, activity completed, another update sent
// * Workflow worker started back up

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	log.Printf("Creating client")
	c, err := client.Dial(client.Options{})
	if err != nil {
		return fmt.Errorf("failed creating client: %w", err)
	}
	defer c.Close()

	taskQueue := uuid.NewString()
	log.Printf("Starting workflow worker", taskQueue)
	worker.SetStickyWorkflowCacheSize(0)
	workflowWorker := worker.New(c, taskQueue, worker.Options{LocalActivityWorkerOnly: true})
	workflowWorker.RegisterWorkflow(MyWorkflow)
	if err := workflowWorker.Start(); err != nil {
		return fmt.Errorf("failed starting worker: %w", err)
	}

	log.Printf("Starting activity worker", taskQueue)
	activityWorker := worker.New(c, taskQueue, worker.Options{DisableWorkflowWorker: true})
	activityWorker.RegisterActivity(MyActivity)
	if err := activityWorker.Start(); err != nil {
		return fmt.Errorf("failed starting worker: %w", err)
	}
	defer activityWorker.Stop()

	log.Printf("Starting workflow")
	run, err := c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{TaskQueue: taskQueue}, MyWorkflow)
	if err != nil {
		return fmt.Errorf("failed starting workflow: %w", err)
	}

	log.Printf("Waiting for activity to show as started")
	<-activityWaiting

	log.Printf("Shutting down workflow worker")
	time.Sleep(2 * time.Second)
	workflowWorker.Stop()

	// Updater
	updateResultCh := make(chan string, 2)
	updateErrCh := make(chan error, 2)
	doUpdate := func() {
		var updateResult string
		if handle, err := c.UpdateWorkflow(ctx, run.GetID(), run.GetRunID(), "MyUpdate"); err != nil {
			updateErrCh <- err
		} else if err := handle.Get(ctx, &updateResult); err != nil {
			updateErrCh <- err
		} else {
			updateResultCh <- updateResult
		}
	}

	log.Printf("Sending first update")
	go doUpdate()

	log.Printf("Waiting two seconds then completing activity")
	time.Sleep(2 * time.Second)
	close(activityShouldComplete)

	log.Printf("Waiting two seconds and sending second update")
	time.Sleep(2 * time.Second)
	go doUpdate()

	log.Printf("Waiting two seconds and starting up workflow worker again", taskQueue)
	time.Sleep(2 * time.Second)
	workflowWorker = worker.New(c, taskQueue, worker.Options{LocalActivityWorkerOnly: true})
	workflowWorker.RegisterWorkflow(MyWorkflow)
	if err := workflowWorker.Start(); err != nil {
		return fmt.Errorf("failed starting worker: %w", err)
	}
	defer workflowWorker.Stop()

	for i := 0; i < 2; i++ {
		select {
		case res := <-updateResultCh:
			log.Printf("Got update result: %v", res)
		case err := <-updateErrCh:
			return fmt.Errorf("got update error: %w", err)
		}
	}

	log.Printf("Replaying workflow")
	replayer := worker.NewWorkflowReplayer()
	replayer.RegisterWorkflow(MyWorkflow)
	err = replayer.ReplayWorkflowExecution(ctx, c.WorkflowService(), nil, "default", workflow.Execution{
		ID:    run.GetID(),
		RunID: run.GetRunID(),
	})
	if err != nil {
		return fmt.Errorf("replay failed: %w", err)
	}
	return nil
}

var activityWaiting = make(chan struct{})
var activityShouldComplete = make(chan struct{})

func MyActivity(ctx context.Context) (string, error) {
	select {
	case <-activityWaiting:
	default:
		close(activityWaiting)
	}
	<-activityShouldComplete
	return "some string", nil
}

func MyWorkflow(ctx workflow.Context) error {
	var updateResponse string

	// Set update handler that only completes after activity completes
	err := workflow.SetUpdateHandlerWithOptions(
		ctx,
		"MyUpdate",
		func(ctx workflow.Context) (string, error) {
			return updateResponse, nil
		},
		workflow.UpdateHandlerOptions{
			Validator: func() error {
				if updateResponse == "" {
					return fmt.Errorf("update response unset")
				}
				return nil
			},
		},
	)
	if err != nil {
		return fmt.Errorf("failed setting update handler: %w", err)
	}

	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		ScheduleToCloseTimeout: 30 * time.Hour,
	})
	if err = workflow.ExecuteActivity(ctx, MyActivity).Get(ctx, &updateResponse); err != nil {
		return fmt.Errorf("failed executing activity: %w", err)
	}

	// Wait around
	return workflow.Sleep(ctx, 30*time.Hour)
}
