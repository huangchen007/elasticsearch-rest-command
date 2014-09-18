package org.elasticsearch.plugin.rest;


import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.cache.Cache;
import org.elasticsearch.common.cache.CacheBuilder;
import org.elasticsearch.common.cache.CacheLoader;
import org.elasticsearch.common.cache.LoadingCache;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.POST;

import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.action.support.RestBuilderListener;
import org.elasticsearch.rest.support.RestUtils;

import com.everdata.command.CommandException;
import com.everdata.command.ReportResponse;
import com.everdata.command.Search;
import com.everdata.command.Search.QueryResponse;
import com.everdata.parser.AST_Start;
import com.everdata.parser.CommandParser;
import com.everdata.parser.ParseException;

public class JobRestHandler extends BaseRestHandler {

	//jobid  -> Search
	//jobid+from+size  -> Query SearchResponse
	//jobid+from+size  -> Report SearchResponse
	
	Random ran = new Random();
	
	Cache<String, String> commandCache = CacheBuilder.newBuilder()
		       .maximumSize(200)
		       .expireAfterAccess(20, TimeUnit.MINUTES)
		       .build();
	
	Cache<String, Response> queryResultCache = CacheBuilder.newBuilder()
		       .maximumSize(2000)
		       .expireAfterAccess(20, TimeUnit.MINUTES)
		       .build();
	
	Cache<String, Response> reportResultCache = CacheBuilder.newBuilder()
		       .maximumSize(2000)
		       .expireAfterAccess(20, TimeUnit.MINUTES)
		       .build();
	
	Cache<String, Response> timelineResultCache = CacheBuilder.newBuilder()
		       .maximumSize(2000)
		       .expireAfterAccess(20, TimeUnit.MINUTES)
		       .build();
	
	private String genJobId(){
		return Double.toString(ran.nextDouble() * 1000000);
	}
	
	static class Id{
		private StringBuffer id = new StringBuffer();
		
		public Id append(String a){
			id.append("-").append(a);
			return this;
		}
		
		public Id append(int a){
			id.append("-").append(Integer.toString(a));
			return this;
		}
		
		public String toId(){
			return id.toString();
		}
	}
	
	static enum ResponseType{
		SearchResponse,QueryResponse,ReportResponse
	}
	
	static class Response{
		ResponseType type;
		SearchResponse search;
		QueryResponse query;
		ReportResponse report;
		List<String> fieldNames = null;
	} 
	
	
	@Inject
	public JobRestHandler(Settings settings, Client client,
			RestController controller) {
		super(settings, client);
		controller.registerHandler(GET, "/_commandjob", this);
		controller.registerHandler(POST, "/_commandjob", this);
		controller.registerHandler(GET, "/jobs/{jobid}/{type}", this);
		controller.registerHandler(GET, "/jobs/{jobid}/{type}", this);
	}
	
	private String getCommandStringFromRestRequest(final RestRequest request) throws CommandException{
		String command = "";
		if(request.method() == RestRequest.Method.GET)
			command = request.param("q", "");
		else{
			HashMap<String, String> post = new HashMap<String, String>();
			RestUtils.decodeQueryString(request.content().toUtf8(), 0, post);
			if(post.containsKey("q")){
				command = post.get("q");
			}
		}
		
		if (command.length() == 0) {
			throw new CommandException("命令为空");
			
		}else{
			if( ! command.startsWith(Search.PREFIX_SEARCH_STRING))
				command = Search.PREFIX_SEARCH_STRING+" "+command;				
		}
		
		logger.info(command);
		
		return command;
	}
	
	private Search newSearchFromCommandString(String command, Client client, RestChannel channel, RestRequest request){
		
		try{
			CommandParser parser = new CommandParser(command);
			
			AST_Start.dumpWithLogger(logger, parser.getInnerTree(), "");
			
			return new Search(parser, client, logger);
		} catch (Exception e1) {
			sendFailure(request, channel, e1);
			return null;
		}
	}

	public static final int DEFAULT_SIZE = 10;
	@Override
	public void handleRequest(final RestRequest request,
			final RestChannel channel, Client client) {
		
		//根据查询命令生成jobid,存储原始的command命令到cache
		if(request.param("jobid") == null ){
			//command支持从GET和POST中获取
			String command;
			
			try {
				command = getCommandStringFromRestRequest(request);
	
			} catch (CommandException e2) {
				sendFailure(request, channel, e2);
				return;
			}
			
			String jobId = genJobId();
			commandCache.put(jobId, command);
			channel.sendResponse(new BytesRestResponse(RestStatus.OK, "{\"jobid\":\"" + jobId +"\"}"));
			return;		
		}
		
		if(request.param("type") == null){
			sendFailure(request, channel, new CommandException("report or query endpoint 需要提供"));
			return;
		}
		String jobid = request.param("jobid");
		String type = request.param("type");
		final int from = request.paramAsInt("from", 0);
		final int size = request.paramAsInt("size", DEFAULT_SIZE);		
		String sortPara = request.param("sortField");
		String interval = request.param("interval");
		String timelineField = request.param("timelineField");
		final boolean showMeta = request.paramAsBoolean("showMeta", false);
		
		Id id = new Id();
		final String retId = id.append(jobid).append(sortPara).append(interval).append(from)
				.append(size).append(timelineField).toId();
		String[] sortFields = new String[0];
		if(sortPara != null)
			sortFields = sortPara.split(",");

		if(type.equalsIgnoreCase("query")){
			
			
			Response response = queryResultCache.getIfPresent(retId);
			
			if(response == null){
				
				final Search search = newSearchFromCommandString(commandCache.getIfPresent(jobid), client, channel, request);
				
				if (search.joinSearchs.size() > 0) {

					search.executeQuery(						
							new ActionListener<QueryResponse>() {
								@Override
								public void onResponse(QueryResponse response) {
									Response resp = new Response();
									resp.type = ResponseType.QueryResponse;
									resp.query = response;
									resp.fieldNames = search.tableFieldNames;
									
									queryResultCache.put(retId, resp);								
									sendQuery(from, request, channel, resp.query, resp.fieldNames, showMeta);
								}
		
								@Override
								public void onFailure(Throwable e) {
									sendFailure(request, channel, e);
								}
							}, from, size, sortFields);
					
				} else {
					
					search.executeQueryWithNonJoin(
							new RestBuilderListener<SearchResponse>(channel) {

								@Override
								public RestResponse buildResponse(SearchResponse result, XContentBuilder builder) throws Exception {
									
									Response resp = new Response();
									resp.type = ResponseType.SearchResponse;
									resp.search = result;
									resp.fieldNames = search.tableFieldNames;
									
									queryResultCache.put(retId, resp);								
									Search.buildQuery(from, builder, result, logger, search.tableFieldNames, showMeta);
									return new BytesRestResponse(RestStatus.OK, builder);
								}

								
								
							}, from, size, sortFields);
					
				}
			
				
			}else if(response.type == ResponseType.QueryResponse){
				sendQuery(from, request, channel, response.query, response.fieldNames, showMeta);
			}else if(response.type == ResponseType.SearchResponse){
				sendQuery(from, request, channel, response.search, response.fieldNames, showMeta);
			}
				
			
				
	
		}else if(type.equalsIgnoreCase("report")){
			
			Response cacheResult = reportResultCache.getIfPresent(retId);
			
			if(cacheResult != null ){				
				sendReport(from, size, request, channel, cacheResult.report);
			} else {
				final Search search = newSearchFromCommandString(commandCache.getIfPresent(jobid), client, channel, request);
				
				
				final ReportResponse result =  new ReportResponse();
				result.bucketFields = search.bucketFields;				
				result.statsFields = search.statsFields;
				
				search.executeReport(
						new ActionListener<SearchResponse>() {
							@Override
							public void onResponse(SearchResponse response) {
								result.response = response;
								
								Response resp = new Response();
								resp.type = ResponseType.ReportResponse;
								resp.report = result;
								reportResultCache.put(retId, resp);
								sendReport(from, size, request, channel, result);
							}
	
							@Override
							public void onFailure(Throwable e) {
								sendFailure(request, channel, e);
							}
						}, from, size);
			}
			
		}else if(type.equalsIgnoreCase("timeline")){
			
			Response cacheResult = timelineResultCache.getIfPresent(retId);
			if(cacheResult != null){
				sendTimeline( request, channel, cacheResult.search);
			} else {
				Search search = newSearchFromCommandString(commandCache.getIfPresent(jobid), client, channel, request);
				
				//快速失败模式，load key
								
				search.executeTimeline(
						new ActionListener<SearchResponse>() {
							@Override
							public void onResponse(SearchResponse response) {
								Response resp = new Response();
								resp.search = response;
								
								timelineResultCache.put(retId, resp);
								sendTimeline(request, channel, response);
							}
	
							@Override
							public void onFailure(Throwable e) {
								sendFailure(request, channel, e);
							}
						}, interval, timelineField);
			}
		}

	}
	
	private void sendQuery(int from, final RestRequest request,final RestChannel channel, SearchResponse response, List<String> fieldNames, boolean showMeta){
		XContentBuilder builder;
		try {
			builder = channel.newBuilder();								
			Search.buildQuery(from, builder, response, logger, fieldNames, showMeta);								
			channel.sendResponse(new BytesRestResponse(RestStatus.OK, builder));
		} catch (IOException e) {
			sendFailure(request, channel, e);
		}
	}
	
	private void sendQuery(int from, final RestRequest request,final RestChannel channel, QueryResponse response, List<String> fieldNames, boolean showMeta){
		XContentBuilder builder;
		try {
			builder = channel.newBuilder();								
			Search.buildQuery(from, builder, response, logger, fieldNames, showMeta);								
			channel.sendResponse(new BytesRestResponse(RestStatus.OK, builder));
		} catch (IOException e) {
			sendFailure(request, channel, e);
		}
	}
	
	

	private void sendReport(int from, int size, final RestRequest request,final RestChannel channel, ReportResponse response){
		XContentBuilder builder;
		try {
			builder = channel.newBuilder();								
			Search.buildReport(from, size, builder, response, logger);								
			channel.sendResponse(new BytesRestResponse(response.response.status(), builder));
		} catch (IOException e) {
			sendFailure(request, channel, e);
		} catch (CommandException e) {
			sendFailure(request, channel, e);
		}
	}
	
	private void sendTimeline(final RestRequest request,final RestChannel channel, SearchResponse response){
		XContentBuilder builder;
		try {
			builder = channel.newBuilder();							
			Search.buildTimeline(builder, response, logger);	
			channel.sendResponse(new BytesRestResponse(response.status(), builder));
		} catch (IOException e) {
			sendFailure(request, channel, e);
		}
	}
	
	public void sendFailure(RestRequest request, RestChannel channel, Throwable e) {
		try {
			channel.sendResponse(new BytesRestResponse(channel, e));
		} catch (IOException e1) {
			logger.error("Failed to send failure response", e1);
		}
	}


}
