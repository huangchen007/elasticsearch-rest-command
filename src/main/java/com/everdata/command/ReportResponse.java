package com.everdata.command;

import java.util.ArrayList;

import org.elasticsearch.action.search.SearchResponse;

public class ReportResponse {
	
	public SearchResponse response = null;	
	public ArrayList<String> bucketFields = null;
	public ArrayList<String> funcFields = null;	
	//public Function countField = null;
	
}
