package com.netflix.conductor.contribs.publisher;

import com.netflix.conductor.common.run.Workflow;
import com.netflix.conductor.core.execution.WorkflowStatusListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;

@Singleton
public class WorkflowStatusPublisher implements WorkflowStatusListener {

    private static final Logger LOGGER = LoggerFactory.getLogger(WorkflowStatusPublisher.class);
    private static final Integer QDEPTH = Integer.parseInt(System.getenv().getOrDefault("ENV_WORKFLOW_NOTIFICATION_QUEUE_SIZE", "50"));
    private BlockingQueue<Workflow> blockingQueue = new LinkedBlockingDeque<>(QDEPTH);
    private RestClientManager rcm;

    class ExceptionHandler implements Thread.UncaughtExceptionHandler
    {
        public void uncaughtException(Thread t, Throwable e)
        {
            LOGGER.info("An exception has been captured\n");
            LOGGER.info("Thread: {}\n", t.getName());
            LOGGER.info("Exception: {}: {}\n", e.getClass().getName(), e.getMessage());
            //LOGGER.info("Stack Trace: \n");
            //e.printStackTrace(System.out);
            LOGGER.info("Thread status: {}\n", t.getState());
            new ConsumerThread().start();
        }
    }

    class ConsumerThread extends Thread {

        public void run(){
            this.setUncaughtExceptionHandler(new ExceptionHandler());
            String tName = Thread.currentThread().getName();
            LOGGER.info("{}: Starting consumer thread", tName);

            WorkflowNotification workflowNotification = null;
            while (true) {
                try {
                    Workflow workflow = blockingQueue.take();
                    workflowNotification = new WorkflowNotification(workflow);
                    String jsonWorkflow = workflowNotification.toJsonString();
                    LOGGER.info("Publishing WorkflowNotification: {}", jsonWorkflow);
                    if (workflowNotification.getAccountMoId().equals("")) {
                        LOGGER.info("Skip workflow '{}' notification. Account Id is empty.", workflowNotification.getWorkflowId());
                        continue;
                    }
                    if (workflowNotification.getDomainGroupMoId().equals("")) {
                        LOGGER.info("Skip workflow '{}' notification. Domain group is empty.", workflowNotification.getWorkflowId());
                        continue;
                    }
                    publishWorkflowNotification(workflowNotification);
                    LOGGER.debug("Workflow {} publish is successful.", workflowNotification.getWorkflowId());
                    Thread.sleep(5);
                }
                catch (Exception e) {
                    if (workflowNotification != null) {
                        LOGGER.error("Failed to publish workflow: {}", workflowNotification.getWorkflowId());
                    } else {
                        LOGGER.error("Failed to publish workflow: Workflow is NULL");
                    }
                    LOGGER.error(e.toString());
                }
            }
        }
    }

    @Inject
    public WorkflowStatusPublisher(RestClientManager rcm) {
        this.rcm = rcm;
        ConsumerThread consumerThread = new ConsumerThread();
        consumerThread.start();
    }

    @Override
    public void onWorkflowCompleted(Workflow workflow) {
        try {
            blockingQueue.put(workflow);
        } catch (Exception e){
            LOGGER.error("Failed to enqueue workflow: Id {} Name {}", workflow.getWorkflowId(), workflow.getWorkflowName());
            LOGGER.error(e.toString());
        }
    }

    @Override
    public void onWorkflowTerminated(Workflow workflow) {
        try {
            blockingQueue.put(workflow);
        } catch (Exception e){
            LOGGER.error("Failed to enqueue workflow: Id {} Name {}", workflow.getWorkflowId(), workflow.getWorkflowName());
            LOGGER.error(e.getMessage());
        }
    }

     private void publishWorkflowNotification(WorkflowNotification workflowNotification) throws IOException {
        String jsonWorkflow = workflowNotification.toJsonString();
        rcm.postNotification(
                RestClientManager.NotificationType.WORKFLOW,
                jsonWorkflow, workflowNotification.getDomainGroupMoId(),
                workflowNotification.getAccountMoId(),
                workflowNotification.getWorkflowId()
                );
    }
}