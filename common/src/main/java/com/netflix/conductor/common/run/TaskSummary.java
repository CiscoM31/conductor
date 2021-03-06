/**
 * Copyright 2016 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/**
 * 
 */
package com.netflix.conductor.common.run;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.TimeZone;

import com.netflix.conductor.common.metadata.tasks.Task;
import com.netflix.conductor.common.metadata.tasks.Task.Status;

/**
 * @author Viren
 *
 */
public class TaskSummary {

	/**
	 * The time should be stored as GMT
	 */
	private static final TimeZone gmt = TimeZone.getTimeZone("GMT");
	
	private String workflowId;
	
	private String taskId;
	
	private String correlationId;
	
	private String scheduledTime;
	
	private String startTime;
	
	private String updateTime;
	
	private String endTime;
	
	private Status status;
	
	private Map<String, Object> inputData;
	
	private String reasonForIncompletion;
	
	private long executionTime;
	
	private long queueWaitTime;
	
	private String taskDefName;
	
	private String taskType;

    private String referenceTaskName;

	public TaskSummary(Task task) {
		
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
    	sdf.setTimeZone(gmt);
    	
    	this.taskDefName = task.getTaskDefName();
    	this.taskType = task.getTaskType();
    	this.referenceTaskName = task.getReferenceTaskName();
		this.workflowId = task.getWorkflowInstanceId();
		this.taskId = task.getTaskId();
		this.inputData = task.getInputData();
		this.correlationId = task.getCorrelationId();
		this.scheduledTime = sdf.format(new Date(task.getScheduledTime()));
		this.startTime = sdf.format(new Date(task.getStartTime()));
		this.updateTime = sdf.format(new Date(task.getUpdateTime()));
		this.endTime = sdf.format(new Date(task.getEndTime()));
		this.status = task.getStatus();
		this.reasonForIncompletion = task.getReasonForIncompletion();
		this.queueWaitTime = task.getQueueWaitTime();

		if(task.getEndTime() > 0){
			this.executionTime = task.getEndTime() - task.getStartTime();
		}
	}

	/**
	 * @return the workflowId
	 */
	public String getWorkflowId() {
		return workflowId;
	}

	/**
	 * @param workflowId the workflowId to set
	 */
	public void setWorkflowId(String workflowId) {
		this.workflowId = workflowId;
	}
	
	/**
	 * @return the taskId
	 */
	public String getTaskId() {
		return taskId;
	}

	/**
	 * @param taskdd the taskId to set
	 */
	public void setTaskId(String taskId) {
		this.taskId = taskId;
	}

	/**
	 * @return the correlationId
	 */
	public String getCorrelationId() {
		return correlationId;
	}

	/**
	 * @param correlationId the correlationId to set
	 */
	public void setCorrelationId(String correlationId) {
		this.correlationId = correlationId;
	}

	/**
	 * @return the scheduledTime
	 */
	public String getScheduledTime() {
		return scheduledTime;
	}

	/**
	 * @param scheduledTime the scheduledTime to set
	 */
	public void setScheduledTime(String scheduledTime) {
		this.scheduledTime = scheduledTime;
	}

	/**
	 * @return the startTime
	 */
	public String getStartTime() {
		return startTime;
	}

	/**
	 * @param startTime the startTime to set
	 * 
	 */
	public void setStartTime(String startTime) {
		this.startTime = startTime;
	}

	/**
	 * @return the updateTime
	 */
	public String getUpdateTime() {
		return updateTime;
	}

	/**
	 * @param updateTime the updateTime to set
	 * 
	 */
	public void setUpdateTime(String updateTime) {
		this.updateTime = updateTime;
	}

	/**
	 * @return the endTime
	 */
	public String getEndTime() {
		return endTime;
	}

	/**
	 * @param endTime the endTime to set
	 * 
	 */
	public void setEndTime(String endTime) {
		this.endTime = endTime;
	}

	/**
	 * @return the status
	 */
	public Status getStatus() {
		return status;
	}

	/**
	 * @param status the status to set
	 * 
	 */
	public void setStatus(Status status) {
		this.status = status;
	}
	
	/**
	 * @return the inputData
	 */
	public Map<String, Object> getInputData() {
		return inputData;
	}
	
	/**
	 * @param inputData the inputData to set
	 * 
	 */
	public void setInputData(Map<String, Object> inputData) {
		this.inputData = inputData;
	}

	/**
	 * @return the reasonForIncompletion
	 */
	public String getReasonForIncompletion() {
		return reasonForIncompletion;
	}

	/**
	 * @param reasonForIncompletion the reasonForIncompletion to set
	 * 
	 */
	public void setReasonForIncompletion(String reasonForIncompletion) {
		this.reasonForIncompletion = reasonForIncompletion;
	}

	/**
	 * @return the executionTime
	 */
	public long getExecutionTime() {
		return executionTime;
	}

	/**
	 * @param executionTime the executionTime to set
	 * 
	 */
	public void setExecutionTime(long executionTime) {
		this.executionTime = executionTime;
	}

	/**
	 * @return the queueWaitTime
	 */
	public long getQueueWaitTime() {
		return queueWaitTime;
	}

	/**
	 * @param queueWaitTime the queueWaitTime to set
	 * 
	 */
	public void setQueueWaitTime(long queueWaitTime) {
		this.queueWaitTime = queueWaitTime;
	}

	/**
	 * @return the taskDefName
	 */
	public String getTaskDefName() {
		return taskDefName;
	}

	/**
	 * @param taskDefName the taskDefName to set
	 * 
	 */
	public void setTaskDefName(String taskDefName) {
		this.taskDefName = taskDefName;
	}

	/**
	 * @return the taskType
	 */
	public String getTaskType() {
		return taskType;
	}

	/**
	 * @param taskType the taskType to set
	 * 
	 */
	public void setTaskType(String taskType) {
		this.taskType = taskType;
	}

	/**
	 * @return the referenceTaskName
	 */
	public String getReferenceTaskName() {
		return referenceTaskName;
	}

	/**
	 * @param referenceTaskName the referenceTaskName to set
	 */
	public void setReferenceTaskName(String referenceTaskName) {
		this.referenceTaskName = referenceTaskName;
	}
}
