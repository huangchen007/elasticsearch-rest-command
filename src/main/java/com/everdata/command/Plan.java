package com.everdata.command;


import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.logging.ESLogger;

public abstract class Plan {

	protected String inputIndexType;
	protected String outputIndexType;
	protected Client client;
	protected ESLogger logger;
	protected int from = 0;
	protected int size = -1;//resultset is all
	
	public Plan(String inputIndexType, String outputIndexType, Client client, ESLogger logger){
		this.inputIndexType = inputIndexType;
		this.outputIndexType = outputIndexType;
		this.client = client;
		this.logger = logger;
	}
	
	public void setResultsetRange(int from, int size){
		this.from = from;
		this.size = size;
		
	}
	/**
	 * Executes the plan and prints applicable output.
	 */
	
	public abstract ActionResponse execute();

} // public interface Plan
