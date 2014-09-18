package com.everdata.command;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Stack;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.deletebyquery.DeleteByQueryRequestBuilder;
import org.elasticsearch.action.deletebyquery.DeleteByQueryResponse;
import org.elasticsearch.action.deletebyquery.IndexDeleteByQueryResponse;
import org.elasticsearch.action.search.ClearScrollRequestBuilder;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.AbstractAggregationBuilder;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogram;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogram.Bucket;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogram.Interval;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramBuilder;
import org.elasticsearch.search.aggregations.metrics.NumericMetricsAggregator.MultiValue;
import org.elasticsearch.search.aggregations.metrics.NumericMetricsAggregator.SingleValue;
import org.elasticsearch.search.aggregations.metrics.avg.Avg;
import org.elasticsearch.search.aggregations.metrics.cardinality.Cardinality;
import org.elasticsearch.search.aggregations.metrics.max.Max;
import org.elasticsearch.search.aggregations.metrics.min.Min;
import org.elasticsearch.search.aggregations.metrics.sum.Sum;
import org.elasticsearch.search.aggregations.metrics.valuecount.ValueCount;
import org.elasticsearch.search.sort.SortOrder;

import com.everdata.command.JoinQuery.Join;
import com.everdata.parser.AST_Join;
import com.everdata.parser.AST_Search;
import com.everdata.parser.AST_Sort;
import com.everdata.parser.AST_Stats;
import com.everdata.parser.AST_Table;
//import com.everdata.parser.AST_Top;
import com.everdata.parser.CommandParser;
import com.everdata.parser.Node;

public class Search {
	public final static String PREFIX_SEARCH_STRING = "SEARCH";

	public static class QueryResponse {
		public long took, totalHits, failedShards, successfulShards,
				totalShards;
		public ArrayList<Map<String, Object>> searchHits = null;

		public QueryResponse(int capcity) {
			searchHits = new ArrayList<Map<String, Object>>(capcity);
		}
	}

	public static final int QUERY = 0;
	public static final int REPORT = 1;
	public static final int TIMELINE = 2;
	public static final int DELETE = 2;

	ESLogger logger;
	Client client;
	String starttime, endtime;
	SearchRequestBuilder querySearch, reportSearch, timelineSearch, cardSearch;
	DeleteByQueryRequestBuilder deleteSearch;

	public ArrayList<Join> joinSearchs = new ArrayList<Join>();

	public ArrayList<com.everdata.parser.AST_Stats.Bucket> bucketFields = null;
	public ArrayList<Function> statsFields = new ArrayList<Function>();
	
	public ArrayList<String> tableFieldNames = new ArrayList<String>(); 

	// public Function countField = null;

	static String[] parseIndices(AST_Search searchTree, Client client) throws CommandException {
		String[] indices = Strings.EMPTY_ARRAY;

		// 过滤不存在的index，不然查询会失败
		// 如果所有指定的index都不存在，那么将在所有的index查询该条件
		String[] originIndices = ((String) searchTree.getOption(Option.INDEX))
				.split(",");
		ArrayList<String> listIndices = new ArrayList<String>();

		for (String index : originIndices) {
			if (client.admin().indices()
					.exists(new IndicesExistsRequest(index)).actionGet()
					.isExists()) {
				listIndices.add(index);
			}
		}

		if (listIndices.size() > 0)
			indices = listIndices.toArray(new String[listIndices.size()]);
		else
			throw new CommandException(String.format("%s index not exsit", searchTree.getOption(Option.INDEX)));
		
		return indices;

	}

	static String[] parseTypes(AST_Search searchTree) {
		String[] sourceTypes = Strings.EMPTY_ARRAY;

		if (searchTree.getOption(Option.SOURCETYPE) != null)
			sourceTypes = ((String) searchTree.getOption(Option.SOURCETYPE))
					.split(",");

		return sourceTypes;
	}

	// 全命令支持
	public Search(CommandParser parser, Client client, ESLogger logger)
			throws CommandException, IOException {

		this.client = client;
		this.logger = logger;

		ArrayList<Node> searchCommands = parser.getSearchCommandList();

		// search rolling out
		AST_Search searchTree = (AST_Search) searchCommands.get(0);

		String[] indices = parseIndices(searchTree, client);
		String[] sourceTypes = parseTypes(searchTree);
		this.querySearch = client.prepareSearch(indices).setTypes(sourceTypes)
				.setQuery(searchTree.getQueryBuilder());
		this.timelineSearch = client.prepareSearch(indices)
				.setTypes(sourceTypes).setQuery(searchTree.getQueryBuilder());
		this.reportSearch = client.prepareSearch(indices).setTypes(sourceTypes)
				.setQuery(searchTree.getQueryBuilder());
		this.cardSearch = client.prepareSearch(indices).setTypes(sourceTypes)
				.setQuery(searchTree.getQueryBuilder());
		this.deleteSearch = client.prepareDeleteByQuery(indices)
				.setTypes(sourceTypes).setQuery(searchTree.getQueryBuilder());

		for (int i = 1; i < searchCommands.size(); i++) {

			if (searchCommands.get(i) instanceof AST_Sort) {
				AST_Sort sortTree = (AST_Sort) searchCommands.get(i);

				for (AST_Sort.SortField field : sortTree.sortFields) {
					addSortToQuery2(field.field, field.desc);
				}

			} else if (searchCommands.get(i) instanceof AST_Join) {
				AST_Join joinTree = (AST_Join) searchCommands.get(i);
				joinSearchs.add(joinTree.getJoin());
			} else if (searchCommands.get(i) instanceof AST_Table) {
				AST_Table tableTree = (AST_Table) searchCommands.get(i);
				if (tableTree.getTables() != null) {
					querySearch.setFetchSource(tableTree.getTables(), null);
					for(String fieldName: tableTree.getTables())
					tableFieldNames.add(fieldName);
				}
			}
		}

		ArrayList<Node> reportCommands = parser.getReportCommandList();

		if (reportCommands.size() > 0) {

			ArrayList<AbstractAggregationBuilder> stats = new ArrayList<AbstractAggregationBuilder>();

			for (Node child : reportCommands) {
				if (child instanceof AST_Stats) {
					
					for (AbstractAggregationBuilder stat : ((AST_Stats) child).getStats()) {
						stats.add(stat);
					}
					
					bucketFields = ((AST_Stats) child).bucketFields();

					statsFields = ((AST_Stats) child).statsFields();
					
					
					for (int idx = 0; idx < bucketFields.size() ; idx ++) {
						
						AST_Stats.Bucket b = bucketFields.get(idx);
							
						if(b.type == AST_Stats.Bucket.TERMWITHCARD){							
							((AST_Stats) child).setBucketLimit(idx, executeCardinary(b.bucketField));
						}
						
					}

				} /*else if (child instanceof AST_Top) {
					stats.add(((AST_Top) child).getTop());

					for (String key : ((AST_Top) child).bucketFields())
						bucketFields.add(key);

					for (String key : ((AST_Top) child).topFields())
						funcFields.add(key);

				} 
				else if (child instanceof AST_Sort) {
					
					for (AST_Sort.SortField field : ((AST_Sort) child).sortFields) {
						addSortToReport(field.field, field.desc);
					}
					
				}*/

			}

			for (AbstractAggregationBuilder stat : stats) {
				reportSearch.addAggregation(stat);
			}

		}

	}
	
	private void addSortToQuery2(String field, boolean desc) {		
		querySearch.addSort(field, desc?SortOrder.DESC:SortOrder.ASC);		
	}

	private void addSortToQuery(String field) {
		boolean desc = true;
		if(field.startsWith("-")){
			desc = true;
			field = field.substring(1);
		}else if(field.startsWith("+")){
			desc = false;
			field = field.substring(1);
		}
		querySearch.addSort(field, desc?SortOrder.DESC:SortOrder.ASC);		
	}
	/*
	private void addSortToReport(String field) {
		
		if (field == null) {
			return;
		} else if (field.startsWith("+")) {
			querySearch.addSort(field.substring(1), SortOrder.ASC);
		} else if (field.startsWith("-")) {
			querySearch.addSort(field.substring(1), SortOrder.DESC);
		} else
			querySearch.addSort(field, SortOrder.ASC);
		
	}
	*/
	public static void buildQuery(int from, XContentBuilder builder,
			SearchResponse response, ESLogger logger, List<String> fieldNames, boolean showMeta) throws IOException {

		logger.info("Query took in millseconds:" + response.getTookInMillis());

		// 格式化结果后输出

		builder.startObject();
		builder.field("took", response.getTookInMillis());
		builder.field("total", response.getHits().getTotalHits());
		builder.field("from", from);

		builder.field("_shard_failed", response.getFailedShards());
		builder.field("_shard_successful", response.getSuccessfulShards());
		builder.field("_shard_total", response.getTotalShards());
		builder.startArray("fields");

		if (response.getHits().getTotalHits() == 0) {
			builder.endArray();
			return;
		}
		
		if(showMeta){
			builder.value("_id");
			builder.value("_index");
			builder.value("_type");
		}
		
		List<String> fields = null;
		
		if(fieldNames != null && fieldNames.size() >0 ){
			fields = fieldNames;
		}else{
			fields = new ArrayList<String>(response.getHits().getAt(0).sourceAsMap().keySet());		
			java.util.Collections.sort(fields);
		}
		
		for (String key : fields) {
			builder.value(key);
		}
		builder.endArray();

		builder.startArray("rows");
		Iterator<SearchHit> iterator = response.getHits().iterator();

		while (iterator.hasNext()) {
			SearchHit _row = iterator.next();

			builder.startArray();
			
			if(showMeta){
				builder.value(_row.getId());
				builder.value(_row.getIndex());
				builder.value(_row.getType());
			}
			
			for (String key : fields) {
				builder.value(_row.sourceAsMap().get(key));
			}

			builder.endArray();

		}
		builder.endArray().endObject();

	}
	public static void buildQuery(int from, XContentBuilder builder,
			QueryResponse response, ESLogger logger, List<String> fieldNames, boolean showMeta) throws IOException {

		logger.info("Query took in millseconds:" + response.took);

		// 格式化结果后输出

		builder.startObject();
		builder.field("took", response.took);
		builder.field("total", response.totalHits);
		builder.field("from", from);

		builder.field("_shard_failed", response.failedShards);
		builder.field("_shard_successful", response.successfulShards);
		builder.field("_shard_total", response.totalShards);
		builder.startArray("fields");

		if (response.searchHits.size() == 0) {
			builder.endArray();
			return;
		}

		List<String> fields = null;
		
		if(fieldNames  != null && fieldNames.size() > 0){
			fields = fieldNames;			
			fields.add(0,"_id");
			fields.add(1,"_index");
			fields.add(2,"_type");
		}else{
			fields = new ArrayList<String>(response.searchHits.get(0)
					.keySet());			
			java.util.Collections.sort(fields);
		}
		
		if(!showMeta){
			fields.remove("_id");
			fields.remove("_index");
			fields.remove("_type");
		}
		
		for (String key : fields) {
			builder.value(key);
		}
		builder.endArray();

		builder.startArray("rows");
		for (Map<String, Object> _row : response.searchHits) {

			builder.startArray();

			for (String key : fields) {
				builder.value(_row.get(key));
			}

			builder.endArray();

		}
		builder.endArray().endObject();

	}

	public void buildDelete(XContentBuilder builder,
			DeleteByQueryResponse response) throws IOException {

		builder.startObject();
		builder.startObject("_indices");
		for (IndexDeleteByQueryResponse indexDeleteByQueryResponse : response
				.getIndices().values()) {
			builder.startObject(indexDeleteByQueryResponse.getIndex(),
					XContentBuilder.FieldCaseConversion.NONE);

			builder.startObject("_shards");
			builder.field("total", indexDeleteByQueryResponse.getTotalShards());
			builder.field("successful",
					indexDeleteByQueryResponse.getSuccessfulShards());
			builder.field("failed",
					indexDeleteByQueryResponse.getFailedShards());
			builder.endObject();

			builder.endObject();
		}
		builder.endObject();
		builder.endObject();
	}

	public static void buildTimeline(XContentBuilder builder,
			SearchResponse response, ESLogger logger) throws IOException {
		logger.info("Report took in millseconds:" + response.getTookInMillis());
		DateHistogram timeline = response.getAggregations().get(
				"data_over_time");

		// 格式化结果后输出

		builder.startObject();
		builder.field("took", response.getTookInMillis());
		builder.field("total", timeline.getBuckets().size());

		builder.startArray("fields");
		builder.value("_bucket_timevalue");
		builder.value("_doc_count");
		builder.endArray();

		builder.startArray("rows");
		for (Bucket bucket : timeline.getBuckets()) {
			builder.startArray();
			builder.value(bucket.getKey());
			builder.value(bucket.getDocCount());
			builder.endArray();
		}
		builder.endArray().endObject();

	}
	
	public static void LeafTraverse(final ArrayList<Function> statsFields, final ArrayList<com.everdata.parser.AST_Stats.Bucket> bucketFields, Queue<List<String>> rows, 
			org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation.Bucket node, Stack<String> path, int[] totalRows,
			final int from, final int size) throws CommandException{
		
		
		path.push(node.getKey());
		
		MultiBucketsAggregation bucketAgg = node.getAggregations().get("statsWithBy");
		if( bucketAgg != null ){
			Iterator<? extends org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation.Bucket> iterator = bucketAgg.getBuckets().iterator();
			while (iterator.hasNext()) {
				LeafTraverse(statsFields, bucketFields, rows, iterator.next(), path, totalRows, from, size);
			}
		}else{
			if(size < 0 ||  totalRows[0] < (from + size) ){
				List<String> row = new ArrayList<String>();
				row.addAll(path);
				
				Map<String, Aggregation> map = node.getAggregations().getAsMap();
	
				for (Function f : statsFields) {
					row.add(getValueFromAggregation(map.get(f.statsField), f));	
				}
				
				row.add(String.valueOf(node.getDocCount()));
	
				rows.add(row);
			}
			totalRows[0] += 1;
		}
		
		path.pop();
	}

	public static void buildReport(int from, int size, XContentBuilder builder,
			ReportResponse response, ESLogger logger) throws IOException,
			CommandException {

		int[] total = {0};
		logger.info("Report took in millseconds:"
				+ response.response.getTookInMillis());

		List<String> fields = new ArrayList<String>();
		LinkedList<List<String>> rows = new LinkedList<List<String>>();
		List<List<String>> reportRows = null;
		Aggregations report = response.response.getAggregations();

		// 构建表头
		for(com.everdata.parser.AST_Stats.Bucket b : response.bucketFields){
			fields.add(b.bucketField);
		}
		
		java.util.Collections.reverse(fields);
		for(Function f: response.statsFields)
			fields.add(f.statsField);

		fields.add("_count");

		if (report.get("statsWithBy") != null) {
			
			MultiBucketsAggregation bucketAgg = report.get("statsWithBy");
			
			Iterator<? extends org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation.Bucket> iterator = bucketAgg.getBuckets().iterator();
			while (iterator.hasNext()) {
				org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation.Bucket next = iterator.next();
				LeafTraverse(response.statsFields, response.bucketFields, rows, next, new Stack<String>(), total, from, size);
			}

		} else {
			// 0级结构
			List<String> row = new ArrayList<String>();

			for (Function f : response.statsFields) {
				row.add(getValueFromAggregation(report.get(f.statsField), f));				
			}

			row.add(String.valueOf(response.response.getHits().getTotalHits()));

			rows.add(row);
			
			total[0] = 1;
		}

		reportRows = rows.subList(from, rows.size());
		
		builder.startObject();
		builder.field("took", response.response.getTookInMillis());
		builder.field("total", total[0]);
		builder.field("from", from);

		builder.field("_shard_failed", response.response.getFailedShards());
		builder.field("_shard_successful",
				response.response.getSuccessfulShards());
		builder.field("_shard_total", response.response.getTotalShards());

		builder.startArray("fields");
		for (String field : fields) {
			builder.value(field);
		}

		builder.endArray();

		builder.startArray("rows");
		for (List<String> row : reportRows) {
			builder.startArray();
			for (String value : row) {
				builder.value(value);
			}
			builder.endArray();
		}
		builder.endArray().endObject();

	}

	public static String getValueFromAggregation(Aggregation a, Function f){
		
		String value = null;
		switch(f.type){
		case Function.SUM :
			value = String.valueOf(((Sum) a).getValue());
			break;
		case Function.COUNT :
			value = String.valueOf(((ValueCount) a).getValue());
			break;
		case Function.DC :
			value = String.valueOf(((Cardinality) a).getValue());
			break;
		case Function.AVG :
			value = String.valueOf(((Avg) a).getValue());
			break;
		case Function.MAX :
			value = String.valueOf(((Max) a).getValue());
			break;
		case Function.MIN :
			value = String.valueOf(((Min) a).getValue());
			break;
		}
		
		return value;
		
		
	}
	
	public void executeQueryWithNonJoin(ActionListener<SearchResponse> listener,
			int from, final int size, String[] sortFields) {

		// jobHandler，执行期才知道要排序
		for (String field : sortFields) {
			if(field != null)addSortToQuery(field);
		}
		querySearch.setSearchType(SearchType.QUERY_THEN_FETCH).setFrom(from).setSize(size);
		dumpSearchScript(querySearch, logger);
		
		querySearch.execute(listener);
	}
	
	public void executeQuery(final ActionListener<QueryResponse> listener,
			int from, final int size, String[] sortFields) {

		// jobHandler，执行期才知道要排序
		for (String field : sortFields) {
			if(field != null)	addSortToQuery(field);
		}

		querySearch.setSearchType(SearchType.QUERY_THEN_FETCH).setFrom(from)
				.setSize(size);
		dumpSearchScript(querySearch, logger);

		SearchResponse response = querySearch.execute().actionGet();
		logger.info(String.format("query took %d", response.getTookInMillis()));
		long milli = System.currentTimeMillis();

		final QueryResponse queryResponse = new QueryResponse(size);

		queryResponse.totalHits = response.getHits().getTotalHits();
		queryResponse.failedShards = response.getFailedShards();
		queryResponse.successfulShards = response.getSuccessfulShards();
		queryResponse.totalShards = response.getTotalShards();
		Iterator<SearchHit> iterator = response.getHits().iterator();

		while (iterator.hasNext()) {
			SearchHit _hit = iterator.next();
			Map<String, Object> hit = _hit.sourceAsMap();
			hit.put("_id", _hit.id());
			hit.put("_index", _hit.index());
			hit.put("_type", _hit.type());

			queryResponse.searchHits.add(hit);
		}

		for (Join join : joinSearchs) {
			try {
				JoinQuery.executeJoin(join, size, queryResponse.searchHits,
						client, logger);
			} catch (CommandException e) {
				logger.error("executeJoin", e);
				listener.onFailure(e);
				return;
			}
		}

		queryResponse.took = response.getTookInMillis()
				+ (System.currentTimeMillis() - milli);
		listener.onResponse(queryResponse);
	}

	public void executeTimeline(final ActionListener<SearchResponse> listener,
			String interval, String timelineField) {
		timelineSearch.setSearchType(SearchType.COUNT);

		DateHistogramBuilder timeline = AggregationBuilders
				.dateHistogram("data_over_time");
		if (timelineField == null)
			timeline.field("_timestamp");
		else
			timeline.field(timelineField);

		try {
			long intervalAtMilli = Long.parseLong(interval);
			timeline.interval(intervalAtMilli);
		} catch (NumberFormatException e) {
			timeline.interval(new Interval(interval));
		}

		timelineSearch.addAggregation(timeline);

		dumpSearchScript(timelineSearch, logger);
		timelineSearch.execute(listener);

	}

	public void executeReport(final ActionListener<SearchResponse> listener,
			int from, int size) {
		reportSearch.setSearchType(SearchType.COUNT);
		dumpSearchScript(reportSearch, logger);
		reportSearch.execute(listener);
	}
	
	private long executeCardinary(String field) throws IOException{
		
		SearchResponse cardResponse = cardSearch.setAggregations(
				JsonXContent.contentBuilder()
				.startObject()
                	.startObject("LIMIT")
                		.startObject("cardinality")
                			.field("field", field)
                		.endObject()
                	.endObject()
				).get();

		Cardinality limit = cardResponse.getAggregations().get("LIMIT");
		
		return limit.getValue();		
	}

	static void dumpSearchScript(SearchRequestBuilder search, ESLogger logger) {

		try {
			XContentBuilder builder = XContentFactory
					.contentBuilder(XContentType.JSON);
			search.internalBuilder().toXContent(builder,
					ToXContent.EMPTY_PARAMS);
			logger.info(builder.bytes().toUtf8());

		} catch (IOException e) {
			logger.info(e.getMessage());
		}

	}

	public void executeDelete(ActionListener<DeleteByQueryResponse> listener) {
		deleteSearch.execute(listener);
	}

	public void executeDownload(final OutputStream httpStream, final XContent xcontent) {

		
		
		final SearchResponse head = querySearch.setSearchType(SearchType.SCAN)
				.setSize(200).setScroll(TimeValue.timeValueMinutes(10))
				.execute().actionGet();

		dumpSearchScript(querySearch, logger);
		
		
		
		new Thread(new Runnable() {

			@Override
			public void run() {

				XContentBuilder builder;
				ClearScrollRequestBuilder clear = client.prepareClearScroll();
				try {
					builder = new XContentBuilder(
							xcontent,
							httpStream);
					builder.startObject();
					builder.field("total", head.getHits().getTotalHits());

					builder.startArray("hits");

					// start scrolling, until we get not results

					String id = head.getScrollId();
					clear.addScrollId(id);

					while (true) {

						SearchResponse result = client.prepareSearchScroll(id)
								.setScroll(TimeValue.timeValueMinutes(10))
								.execute().actionGet();

						if (result.getHits().hits().length == 0) {
							break;
						}

						for (SearchHit hit : result.getHits()) {
							hit.toXContent(builder, null);
						}
						logger.info("executeDownload scrollId: " + id);
						clear.addScrollId(id);
						id = result.getScrollId();

					}
					builder.endArray();
					builder.endObject();
					builder.close();

					clear.get();

				} catch (Exception e) {
					logger.error("executeDownload error", e);
				}
			}

		}).start();

		return;

	}



}
