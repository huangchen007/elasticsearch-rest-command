package org.elasticsearch.plugin.rest;


import java.io.IOException;
import java.io.OutputStream;
import java.util.HashMap;

import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.XContentRestResponse;
import org.elasticsearch.rest.XContentThrowableRestResponse;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.deletebyquery.DeleteByQueryResponse;
import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.common.xcontent.XContentBuilder;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.rest.action.support.RestXContentBuilder.restContentBuilder;

import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.action.support.RestXContentBuilder;
import org.elasticsearch.rest.support.RestUtils;

import com.everdata.command.CommandException;
import com.everdata.command.ReportResponse;
import com.everdata.command.Search;
import com.everdata.parser.AST_Start;
import com.everdata.parser.CommandParser;
import com.everdata.parser.ParseException;

public class CommandRestHandler extends BaseRestHandler {

	@Inject
	public CommandRestHandler(Settings settings, Client client,
			RestController controller) {
		super(settings, client);
		controller.registerHandler(GET, "/_command", this);
		controller.registerHandler(POST, "/_command", this);
	}

	@Override
	public void handleRequest(final RestRequest request,
			final RestChannel channel) {

		
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
			SendFailure(request, channel,new CommandException("命令为空"));
			return;
		}else{
			if( ! command.toLowerCase().startsWith("search"))
				command = "search "+command;				
		}
		logger.info(command);

		final int from = request.paramAsInt("from", 0);
		final int size = request.paramAsInt("size", 50);

		final Search search;

		try {
			CommandParser parser = new CommandParser(command);
			
			AST_Start.dumpWithLogger(logger, parser.getInnerTree(), "");
			
			search = new Search(parser, client, logger);
			

		} catch (CommandException e2) {
			SendFailure(request, channel, e2);
			return;
		} catch (ParseException e1) {
			SendFailure(request, channel, e1);
			return;
		}

		if (request.paramAsBoolean("delete", false)) {
			search.executeDelete(new ActionListener<DeleteByQueryResponse>() {

				@Override
				public void onFailure(Throwable e) {
					SendFailure(request, channel, e);
				}

				@Override
				public void onResponse(DeleteByQueryResponse response) {

					try {
						XContentBuilder builder = RestXContentBuilder
								.restContentBuilder(request);
						search.buildDelete(builder, response);
						channel.sendResponse(new XContentRestResponse(request,
								response.status(), builder));
					} catch (Throwable e) {
						onFailure(e);
					}
				}

			});

			return;
		}

		if (request.paramAsBoolean("download", false)) {

			search.executeDownload(new OutputStream() {
				byte[] innerBuffer = new byte[1200];
				int idx = 0;
				@Override
				public void write(int b) throws IOException {
					innerBuffer[idx++] = (byte) b;
					if (idx == innerBuffer.length) {
						channel.sendBytes(innerBuffer, 0, idx, false);
						idx = 0;
					}
				}
				@Override
				public void close() throws IOException {
					if (idx > 0)
						channel.sendBytes(innerBuffer, 0, idx, true);
					else
						channel.sendBytes(null, 0, 0, true);
				}

			});

		} else {
			if( request.paramAsBoolean("query", true) ){

				search.executeQuery(
						new ActionListener<SearchResponse>() {
							@Override
							public void onResponse(SearchResponse response) {
								try {
									XContentBuilder builder = restContentBuilder(request);
									Search.buildQuery(from, builder, response, logger);									
									channel.sendResponse(new XContentRestResponse(
											request, response.status(), builder));
								} catch (Exception e) {								
									onFailure(e);
								}
							}
	
							@Override
							public void onFailure(Throwable e) {
								SendFailure(request, channel, e);
							}
						}, from, size);
			}else{
				final ReportResponse result =  new ReportResponse();
				result.bucketFields = search.bucketFields;

				result.funcFields = search.funcFields;
				
				search.executeReport(
						new ActionListener<SearchResponse>() {
							@Override
							public void onResponse(SearchResponse response) {
								try {
									XContentBuilder builder = restContentBuilder(request);
									
									result.response = response;
									Search.buildReport(from, builder, result, logger);
									channel.sendResponse(new XContentRestResponse(
											request, response.status(), builder));
								} catch (Exception e) {
									onFailure(e);
								}
							}
	
							@Override
							public void onFailure(Throwable e) {
								SendFailure(request, channel, e);
							}
						}, from, size);
			}
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
