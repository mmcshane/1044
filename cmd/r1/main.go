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
// * Workflow waits for activity inside update
// * Workflow worker stops after activity started
// * Update sent
// * Activity completed
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

	log.Printf("Starting workflow worker")
	taskQueue := uuid.NewString()
	workflowWorker := worker.New(c, taskQueue, worker.Options{LocalActivityWorkerOnly: true})
	workflowWorker.RegisterWorkflow(MyWorkflow)
	if err := workflowWorker.Start(); err != nil {
		return fmt.Errorf("failed starting worker: %w", err)
	}

	log.Printf("Starting activity worker")
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

	log.Printf("Starting update")
	// Send update in background
	updateResultCh := make(chan string, 1)
	updateErrCh := make(chan error, 1)
	go func() {
		var updateResult string
		handle, err := c.UpdateWorkflow(ctx, run.GetID(), run.GetRunID(), "MyUpdate")
		if err != nil {
			updateErrCh <- err
		} else if err := handle.Get(ctx, &updateResult); err != nil {
			updateErrCh <- err
		} else {
			updateResultCh <- updateResult
		}
	}()

	log.Printf("Waiting two seconds then shutting down workflow worker")
	time.Sleep(2 * time.Second)
	workflowWorker.Stop()

	log.Printf("Completing activity")
	close(activityShouldComplete)

	log.Printf("Waiting two seconds and starting up workflow worker again")
	time.Sleep(2 * time.Second)
	workflowWorker = worker.New(c, taskQueue, worker.Options{LocalActivityWorkerOnly: true})
	workflowWorker.RegisterWorkflow(MyWorkflow)
	if err := workflowWorker.Start(); err != nil {
		return fmt.Errorf("failed starting worker: %w", err)
	}
	defer workflowWorker.Stop()

	select {
	case res := <-updateResultCh:
		log.Printf("Got update result: %v", res)
		return nil
	case err := <-updateErrCh:
		return fmt.Errorf("got update error: %w", err)
	}
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
	// Start activity
	fut := workflow.ExecuteActivity(
		workflow.WithActivityOptions(ctx, workflow.ActivityOptions{ScheduleToCloseTimeout: 30 * time.Hour}),
		MyActivity,
	)

	// Set update handler that only completes after activity completes
	err := workflow.SetUpdateHandler(
		ctx,
		"MyUpdate",
		func(ctx workflow.Context) (s string, err error) {
			err = fut.Get(ctx, &s)
			return
		},
	)
	if err != nil {
		return fmt.Errorf("failed setting update handler: %w", err)
	}

	// Wait around
	return workflow.Sleep(ctx, 30*time.Hour)
}
