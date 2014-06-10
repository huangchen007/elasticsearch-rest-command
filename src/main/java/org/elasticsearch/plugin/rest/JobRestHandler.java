package org.elasticsearch.plugin.rest;


import java.io.IOException;
import java.util.HashMap;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.cache.CacheBuilder;
import org.elasticsearch.common.cache.CacheLoader;
import org.elasticsearch.common.cache.LoadingCache;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.POST;

import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.support.RestUtils;

import com.everdata.command.CommandException;
import com.everdata.command.ReportResponse;
import com.everdata.command.Search;
import com.everdata.parser.AST_Start;
import com.everdata.parser.CommandParser;
import com.everdata.parser.ParseException;

public class JobRestHandler extends BaseRestHandler {

	//jobid  -> Search
	//jobid+from+size  -> Query SearchResponse
	//jobid+from+size  -> Report SearchResponse
	
	Random ran = new Random();
	
	LoadingCache<String, String> commandCache = CacheBuilder.newBuilder()
		       .maximumSize(200)
		       .expireAfterAccess(20, TimeUnit.MINUTES)
		       .build(
		           new CacheLoader<String, String>() {
		             public String load(String jobid) throws CommandException {
		               throw new CommandException("不存在的jobid，确认有这个id吗？");
		             }
		           });
	
	LoadingCache<String, SearchResponse> queryResultCache = CacheBuilder.newBuilder()
		       .maximumSize(2000)
		       .expireAfterAccess(20, TimeUnit.MINUTES)
		       .build(
		           new CacheLoader<String, SearchResponse>() {
		             public SearchResponse load(String jobidWithFromSize) throws CommandException {
		            	 //fast failure mode	快速失败模式
		               throw new CommandException("不存在的key，确认有这个id吗？");
		             }
		           });
	
	LoadingCache<String, ReportResponse> reportResultCache = CacheBuilder.newBuilder()
		       .maximumSize(2000)
		       .expireAfterAccess(20, TimeUnit.MINUTES)
		       .build(
		           new CacheLoader<String, ReportResponse>() {
		             public ReportResponse load(String jobidWithFromSize) throws CommandException {
		            	//fast failure mode	快速失败模式
		               throw new CommandException("不存在的key，确认有这个id吗？");
		             }
		           });
	
	LoadingCache<String, SearchResponse> timelineResultCache = CacheBuilder.newBuilder()
		       .maximumSize(2000)
		       .expireAfterAccess(20, TimeUnit.MINUTES)
		       .build(
		           new CacheLoader<String, SearchResponse>() {
		             public SearchResponse load(String jobidWithFromSize) throws CommandException {
		            	 //fast failure mode	快速失败模式
		               throw new CommandException("不存在的key，确认有这个id吗？");
		             }
		           });
	
	private String genJobId(){
		return Double.toString(ran.nextDouble() * 1000000);
	}
	class Id{
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
			if( ! command.toLowerCase().startsWith("search"))
				command = "search "+command;				
		}
		
		logger.info(command);
		
		return command;
	}
	
	private Search newSearchFromCommandString(String command) throws ParseException, CommandException{
		CommandParser parser = new CommandParser(command);
		
		AST_Start.dumpWithLogger(logger, parser.getInnerTree(), "");
		
		return new Search(parser, client, logger);
	}

	@Override
	public void handleRequest(final RestRequest request,
			final RestChannel channel) {
		
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
		int size = request.paramAsInt("size", 50);
		String sortField = request.param("sortField");
		String interval = request.param("interval");
		String timelineField = request.param("timelineField");
		
		Id id = new Id();
		final String retId = id.append(jobid).append(sortField).append(interval).append(from)
				.append(size).append(timelineField).toId();
		

		if(type.equalsIgnoreCase("query")){
			
			try{
				SearchResponse cacheResult = queryResultCache.get(retId);
				sendQuery(from, request, channel, cacheResult);
			} catch (ExecutionException e) {
				Search search;
				try {
					search = newSearchFromCommandString(commandCache.get(jobid));
				} catch (Exception e1) {
					sendFailure(request, channel, e1);
					return;
				}
				
				//快速失败模式，load key
				
				search.executeQuery(
						new ActionListener<SearchResponse>() {
							@Override
							public void onResponse(SearchResponse response) {								
								queryResultCache.put(retId, response);								
								sendQuery(from, request, channel, response);
							}
	
							@Override
							public void onFailure(Throwable e) {
								sendFailure(request, channel, e);
							}
						}, from, size, sortField);
			}
	
		}else if(type.equalsIgnoreCase("report")){
			try{
				ReportResponse cacheResult = reportResultCache.get(retId);
				sendReport(from, request, channel, cacheResult);
			} catch (ExecutionException e) {
				Search search;
				try {
					search = newSearchFromCommandString(commandCache.get(jobid));
				} catch (Exception e1) {
					sendFailure(request, channel, e1);
					return;
				}
				//快速失败模式，load key
				final ReportResponse result =  new ReportResponse();
				result.bucketFields = search.bucketFields;
				
				result.funcFields = search.funcFields;
				
				search.executeReport(
						new ActionListener<SearchResponse>() {
							@Override
							public void onResponse(SearchResponse response) {
								result.response = response;
								reportResultCache.put(retId, result);
								sendReport(from, request, channel, result);
							}
	
							@Override
							public void onFailure(Throwable e) {
								sendFailure(request, channel, e);
							}
						}, from, size);
			}
		}else if(type.equalsIgnoreCase("timeline")){
			try{
				SearchResponse cacheResult = timelineResultCache.get(retId);
				sendTimeline( request, channel, cacheResult);
			} catch (ExecutionException e) {
				Search search;
				try {
					search = newSearchFromCommandString(commandCache.get(jobid));
				} catch (Exception e1) {
					sendFailure(request, channel, e1);
					return;
				}
				//快速失败模式，load key
								
				search.executeTimeline(
						new ActionListener<SearchResponse>() {
							@Override
							public void onResponse(SearchResponse response) {
								
								timelineResultCache.put(retId, response);
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
	
	
	private void sendQuery(int from, final RestRequest request,final RestChannel channel, SearchResponse response){
		XContentBuilder builder;
		try {
			builder = channel.newBuilder();								
			Search.buildQuery(from, builder, response, logger);								
			channel.sendResponse(new BytesRestResponse(response.status(), builder));
		} catch (IOException e) {
			sendFailure(request, channel, e);
		}
	}
	
	

	private void sendReport(int from, final RestRequest request,final RestChannel channel, ReportResponse response){
		XContentBuilder builder;
		try {
			builder = channel.newBuilder();								
			Search.buildReport(from, builder, response, logger);								
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
