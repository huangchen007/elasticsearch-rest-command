package org.elasticsearch.plugin.rest;


import java.io.IOException;
import java.util.HashMap;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.StringRestResponse;
import org.elasticsearch.rest.XContentRestResponse;
import org.elasticsearch.rest.XContentThrowableRestResponse;
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
import static org.elasticsearch.rest.action.support.RestXContentBuilder.restContentBuilder;

import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.support.RestUtils;

import com.everdata.command.CommandException;
import com.everdata.command.Search;
import com.everdata.parser.AST_Start;
import com.everdata.parser.CommandParser;
import com.everdata.parser.ParseException;

public class JobRestHandler extends BaseRestHandler {

	//jobid  -> Search
	//jobid+from+size  -> Query SearchResponse
	//jobid+from+size  -> Report SearchResponse
	
	Random ran = new Random();
	
	LoadingCache<String, Search> searchCache = CacheBuilder.newBuilder()
		       .maximumSize(200)
		       .expireAfterAccess(20, TimeUnit.MINUTES)
		       .build(
		           new CacheLoader<String, Search>() {
		             public Search load(String jobid) throws CommandException {
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
	
	LoadingCache<String, SearchResponse> reportResultCache = CacheBuilder.newBuilder()
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
	
	private String genRetId(String jobId, int from, int size){
		return new StringBuffer(jobId).append("-").append(from).append("-").append(size).toString();
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

	@Override
	public void handleRequest(final RestRequest request,
			final RestChannel channel) {

		if(request.param("jobid") == null ){

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
				SendFailure(request, channel, new CommandException("命令为空"));
				return;
			}else{
				if( ! command.toLowerCase().startsWith("search"))
					command = "search "+command;				
			}
	
	
	
			Search search = null;
	
			try {
				CommandParser parser = new CommandParser(command);
				
				if (logger.isDebugEnabled()) {
					AST_Start.dumpWithLogger(logger, parser.getInnerTree(), "");
				}
	
				search = new Search(parser, client, logger);
				
	
			} catch (CommandException e2) {
				SendFailure(request, channel, e2);
				return;
			} catch (ParseException e1) {
				SendFailure(request, channel, e1);
				return;
			}
			
			String jobId = genJobId();
			searchCache.put(jobId, search);
			channel.sendResponse(new StringRestResponse(RestStatus.OK, "{\"jobid\":\"" + jobId +"\"}"));
			return;
		
		}
		
		if(request.param("type") == null){
			SendFailure(request, channel, new CommandException("report or query endpoint 需要提供"));
			return;
		}
		String jobid = request.param("jobid");
		String type = request.param("type");
		final int from = request.paramAsInt("from", 0);
		int size = request.paramAsInt("size", 50);
		
		
		final Search search;
		try {
			search = searchCache.get(jobid);
		} catch (ExecutionException e1) {
			SendFailure(request, channel, e1);
			return;
		}
		
		final String retId = genRetId(jobid, from, size);
		SearchResponse result = null;

		if(type.equalsIgnoreCase("query")){
			
			try{
				result = queryResultCache.get(retId);
				sendQuery(from, search, request, channel, result);
			} catch (ExecutionException e) {
				//快速失败模式，load key
				search.executeQuery(
						new ActionListener<SearchResponse>() {
							@Override
							public void onResponse(SearchResponse response) {								
								queryResultCache.put(retId, response);								
								sendQuery(from, search, request, channel, response);
							}
	
							@Override
							public void onFailure(Throwable e) {
								SendFailure(request, channel, e);
							}
						}, from, size);
			}
				
				
				
				
		}else if(type.equalsIgnoreCase("report")){
			try{
				result = reportResultCache.get(retId);
				sendReport(from, search, request, channel, result);
			} catch (ExecutionException e) {
				//快速失败模式，load key
				search.executeReport(
						new ActionListener<SearchResponse>() {
							@Override
							public void onResponse(SearchResponse response) {
								reportResultCache.put(retId, response);
								sendReport(from, search, request, channel, response);
							}
	
							@Override
							public void onFailure(Throwable e) {
								SendFailure(request, channel, e);
							}
						}, from, size);
			}
		}

	}
	
	
	private void sendQuery(int from, Search search, final RestRequest request,final RestChannel channel, SearchResponse response){
		XContentBuilder builder;
		try {
			builder = restContentBuilder(request);								
			search.buildQuery(from, builder, response);								
			channel.sendResponse(new XContentRestResponse(request, response.status(), builder));
		} catch (IOException e) {
			SendFailure(request, channel, e);
		}
	}
	
	

	private void sendReport(int from, Search search, final RestRequest request,final RestChannel channel, SearchResponse response){
		XContentBuilder builder;
		try {
			builder = restContentBuilder(request);								
			search.buildReport(from, builder, response);								
			channel.sendResponse(new XContentRestResponse(request, response.status(), builder));
		} catch (IOException e) {
			SendFailure(request, channel, e);
		} catch (CommandException e) {
			SendFailure(request, channel, e);
		}
	}


        


	public void SendFailure(RestRequest request, RestChannel channel, Throwable e) {
		try {
			channel.sendResponse(new XContentThrowableRestResponse(request, e));
		} catch (IOException e1) {
			logger.error("Failed to send failure response", e1);
		}
	}


}
