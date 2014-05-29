package com.everdata.command;



import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

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
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.search.SearchHit;







import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.AbstractAggregationBuilder;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.filter.Filter;




import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogram;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogram.Bucket;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogram.Interval;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramBuilder;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsBuilder;
import org.elasticsearch.search.aggregations.bucket.terms.Terms.Order;
import org.elasticsearch.search.aggregations.metrics.MetricsAggregation.MultiValue;
import org.elasticsearch.search.aggregations.metrics.MetricsAggregation.SingleValue;
import org.elasticsearch.search.aggregations.metrics.valuecount.ValueCount;
import org.elasticsearch.search.sort.SortOrder;

import com.everdata.parser.AST_Search;
import com.everdata.parser.AST_Sort;
import com.everdata.parser.AST_Stats;
import com.everdata.parser.AST_Top;
import com.everdata.parser.CommandParser;
import com.everdata.parser.Node;



public class Search {

	public static final int QUERY = 0;
	public static final int REPORT = 1;
	public static final int TIMELINE = 2;
	public static final int DELETE = 2;
	
	ESLogger logger;
	Client client;
	String starttime, endtime;
	SearchRequestBuilder querySearch, reportSearch, timelineSearch;
	DeleteByQueryRequestBuilder deleteSearch;
	
	public ArrayList<String> bucketFields = new ArrayList<String>();
	public ArrayList<String> funcFields = new ArrayList<String>();
	
	//public Function countField = null;

	public Search(CommandParser parser, Client client, ESLogger logger) throws CommandException{
		
		this.client = client;
		this.logger = logger;
				
		String[] indices = Strings.EMPTY_ARRAY;
		String[] sourceTypes = Strings.EMPTY_ARRAY;
		
		ArrayList<Node> searchCommands = parser.getSearchCommandList();
		
		//search rolling out
		AST_Search searchTree = (AST_Search) searchCommands.get(0);		
		
		//过滤不存在的index，不然查询会失败
		//如果所有指定的index都不存在，那么将在所有的index查询该条件
		String[] originIndices = searchTree.getOption(Option.INDEX).split(",");
		ArrayList<String> listIndices = new ArrayList<String>();
		
		for(String index: originIndices){
			if(client.admin().indices().exists(new IndicesExistsRequest(index)).actionGet().isExists()){
				listIndices.add(index);
			}
		}
		
		if(listIndices.size() > 0)
			indices = listIndices.toArray(new String[listIndices.size()]);
		
		
		if(searchTree.getOption(Option.SOURCETYPE) != null)
			sourceTypes = searchTree.getOption(Option.SOURCETYPE).split(",");
				
		this.querySearch = client.prepareSearch(indices).setTypes(sourceTypes)
				.setQuery(searchTree.getQueryBuilder());
		this.timelineSearch = client.prepareSearch(indices).setTypes(sourceTypes)
				.setQuery(searchTree.getQueryBuilder());
		this.reportSearch = client.prepareSearch(indices).setTypes(sourceTypes)
				.setQuery(searchTree.getQueryBuilder());
		this.deleteSearch = client.prepareDeleteByQuery(indices).setTypes(sourceTypes)
				.setQuery(searchTree.getQueryBuilder());
		
		
		for(int i = 1; i < searchCommands.size(); i++){
			
			if (searchCommands.get(i) instanceof AST_Sort){
				AST_Sort sortTree = (AST_Sort) 	searchCommands.get(i);
				
				for(String field: sortTree.sortFields){					
					addSortToQuery(field);					
				}

			}
		}
		
		ArrayList<Node> reportCommands = parser.getReportCommandList();
		
		if(reportCommands.size() > 0){
			
			
			ArrayList<AbstractAggregationBuilder> stats = new ArrayList<AbstractAggregationBuilder>();
			
			//Order order = new Order();
			
			for (Node child : reportCommands) {
				if (child instanceof AST_Stats){
					
					for(AbstractAggregationBuilder stat: ((AST_Stats) child).getStats()){
						stats.add(stat);						
					}
					for(String key:((AST_Stats) child).bucketFields())
						bucketFields.add(key);
					
					for(String key:((AST_Stats) child).statsFields())
						funcFields.add(key);
					
					//countField = ((AST_Stats) child).count;
					
				}else if (child instanceof AST_Top){
					stats.add(((AST_Top) child).getTop());
					
					for(String key:((AST_Top) child).bucketFields())
						bucketFields.add(key);
					
					for(String key:((AST_Top) child).topFields())
						funcFields.add(key);
					
				}else if (child instanceof AST_Sort){
					//((AST_Sort) child).s;
				}

			}
			
			
			for(AbstractAggregationBuilder stat: stats){
				reportSearch.addAggregation(stat);
			}
			
		}
				
		
	}
	
	private void addSortToQuery(String field){
		if(field == null){
			return;
		}else if(field.startsWith("+")){
			querySearch.addSort(field.substring(1), SortOrder.ASC);
		}else if(field.startsWith("-")){
			querySearch.addSort(field.substring(1), SortOrder.DESC);
		}else
			querySearch.addSort(field, SortOrder.ASC);
	}
	
	public static void buildQuery(int from, XContentBuilder builder, SearchResponse response, ESLogger logger)
			throws IOException {

		HashSet<String> fields = new HashSet<String>();

		SearchHits hits = response.getHits();
		
		logger.info("Query took in millseconds:" + response.getTookInMillis());

		// create fields head

		SearchHit hit = null;

		Iterator<SearchHit> iterator = hits.iterator();
		while (iterator.hasNext()) {
			hit = iterator.next();
			fields.addAll(hit.sourceAsMap().keySet());
		}

		// 格式化结果后输出

		builder.startObject();
		builder.field("took", response.getTookInMillis());
		builder.field("total", hits.getTotalHits());
		builder.field("from", from);
		
		builder.field("_shard_failed", response.getFailedShards());
		builder.field("_shard_successful", response.getSuccessfulShards());
		builder.field("_shard_total", response.getTotalShards());
		builder.startArray("fields");
		builder.value("_id");
		builder.value("_index");
		builder.value("_type");
		for (String key : fields) {
			builder.value( key);
		}
		builder.endArray();
		
		

		builder.startArray("rows");
		iterator = hits.iterator();
		while (iterator.hasNext()) {
			hit = iterator.next();
			builder.startArray();
			builder.value(hit.id());
			builder.value(hit.index());
			builder.value(hit.type());
			
			for (String key : fields) {
				if(hit.isSourceEmpty())
					builder.value("");
				else
					builder.value(hit.sourceAsMap().get(key));				
			}
			
			builder.endArray();

		}
		builder.endArray().endObject();

	}
	
	public void buildDelete(XContentBuilder builder,	DeleteByQueryResponse response) throws IOException{
		
		builder.startObject();
		builder.startObject("_indices");
		for (IndexDeleteByQueryResponse indexDeleteByQueryResponse : response
				.getIndices().values()) {
			builder.startObject(
					indexDeleteByQueryResponse.getIndex(),
					XContentBuilder.FieldCaseConversion.NONE);

			builder.startObject("_shards");
			builder.field("total",
					indexDeleteByQueryResponse.getTotalShards());
			builder.field("successful",
					indexDeleteByQueryResponse
							.getSuccessfulShards());
			builder.field("failed", indexDeleteByQueryResponse
					.getFailedShards());
			builder.endObject();

			builder.endObject();
		}
		builder.endObject();
		builder.endObject();
	}
	public static void buildTimeline(XContentBuilder builder, SearchResponse response, ESLogger logger) throws IOException{
		logger.info("Report took in millseconds:" + response.getTookInMillis());
		DateHistogram timeline = response.getAggregations().get("data_over_time");
		
		// 格式化结果后输出

		builder.startObject();
		builder.field("took", response.getTookInMillis());
		builder.field("total", timeline.getBuckets().size());

		builder.startArray("fields");
		builder.value("_bucket_timevalue");
		builder.value("_doc_count");
		builder.endArray();
		
		builder.startArray("rows");
		for(Bucket bucket: timeline.getBuckets()){
			builder.startArray();
			builder.value(bucket.getKey());			
			builder.value(bucket.getDocCount());
			builder.endArray();
		}
		builder.endArray().endObject();

	}
	
	public static void buildReport(int from, XContentBuilder builder,
			ReportResponse response, ESLogger logger) throws IOException, CommandException {

		logger.debug("Report took in millseconds:" + response.response.getTookInMillis());
		
		List<String> fields = new ArrayList<String>();
		List<List<String>> rows = new ArrayList<List<String>>();

		Aggregations report = response.response.getAggregations();
		
		//构建表头
		fields.addAll(response.bucketFields);
		fields.addAll(response.funcFields);
		
		
		fields.add("_count");
		
		
		if (report.get("statsWithBy") != null) {
			// 一级结构
			Terms terms = report.get("statsWithBy");

			Iterator<Terms.Bucket> iterator = terms.getBuckets().iterator();
			while (iterator.hasNext()) {
				List<String> row = new ArrayList<String>();
				
				Terms.Bucket next = iterator.next();

				for (String value : next.getKey().split("\\|"))
					// 分组Key的具体值，e.g: www.sina.com, 多值 www.sina.com|80|123
					row.add(value);

				Map<String, Aggregation> map = next.getAggregations()
						.getAsMap();


				for (String k : response.funcFields) {
					Aggregation agg = map.get(k);

					if (agg instanceof SingleValue) {
						double value = ((SingleValue) agg).value();
						row.add(String.valueOf(value));
					} else if (agg instanceof MultiValue) {
						throw new CommandException("暂不支持 MultiValue");
					}

				}
				
				
				row.add(String.valueOf(next.getDocCount()));
				
				rows.add(row);
			}

		} else if (report.get("topWithBy") != null) {
			// 两级bucket结构
			
			Terms terms = report.get("topWithBy");

			Iterator<Terms.Bucket> firstIterator = terms.getBuckets().iterator();
			while (firstIterator.hasNext()) {
				
				
				Terms.Bucket firstBucket = firstIterator.next();


				Iterator<Terms.Bucket> secondIterator = 
						((Terms)firstBucket.getAggregations().get("top")).getBuckets().iterator();
				
				while(secondIterator.hasNext()){
					
					Terms.Bucket secondBucket = secondIterator.next();
					List<String> row = new ArrayList<String>();
					
					for (String value : firstBucket.getKey().split("\\|"))
						// 分组Key的具体值，e.g: www.sina.com, 多值 www.sina.com|80|123
						row.add(value);
					
					for (String value2 : secondBucket.getKey().split("\\|"))
						// 分组Key的具体值，e.g: www.sina.com, 多值 www.sina.com|80|123
						row.add(value2);
					
					row.add(String.valueOf(secondBucket.getDocCount()));
					rows.add(row);
				}				
			}
		} else if (report.get("top") != null) {
			// 一级结构
			Terms terms = report.get("top");

			Iterator<Terms.Bucket> firstIterator = terms.getBuckets().iterator();
			while (firstIterator.hasNext()) {
				List<String> row = new ArrayList<String>();
				
				Terms.Bucket firstBucket = firstIterator.next();

				for (String value : firstBucket.getKey().split("\\|"))
						// 分组Key的具体值，e.g: www.sina.com, 多值 www.sina.com|80|123
						row.add(value);
				
				
				row.add(String.valueOf(firstBucket.getDocCount()));
				rows.add(row);
								
			}
		} else {
			// 0级结构
			List<String> row = new ArrayList<String>();
			
			for (String k : response.funcFields) {
				Aggregation a = report.get(k);
				if(a instanceof SingleValue)
					row.add(String.valueOf(((SingleValue)a).value()));
				else if(a instanceof ValueCount)
					row.add(String.valueOf(((ValueCount)a).getValue()));
			}
			
			row.add(String.valueOf(response.response.getHits().getTotalHits()));
			
			rows.add(row);
		}

		builder.startObject();
		builder.field("took", response.response.getTookInMillis());
		builder.field("total", rows.size());
		builder.field("from", from);

		builder.field("_shard_failed", response.response.getFailedShards());
		builder.field("_shard_successful", response.response.getSuccessfulShards());
		builder.field("_shard_total", response.response.getTotalShards());
		
		builder.startArray("fields");
		for (String field : fields) {
			builder.value(field);
		}

		builder.endArray();

		builder.startArray("rows");
		for (List<String> row : rows) {
			builder.startArray();
			for (String value : row) {
				builder.value(value);
			}
			builder.endArray();
		}
		builder.endArray().endObject();

	}
	
	public void executeQuery(final ActionListener<SearchResponse> listener, int from, int size, String ...sortFields) {
		
		for(String sort: sortFields){
			addSortToQuery(sort);
		}

		querySearch.setSearchType(SearchType.QUERY_THEN_FETCH)
				.setFrom(from)
				.setSize(size);
		dumpSearchScriptWhenDebug(querySearch);
		
		querySearch.execute(listener);

	}
	
	public void executeTimeline(final ActionListener<SearchResponse> listener, String interval, String timelineField){
		timelineSearch.setSearchType(SearchType.COUNT);

		DateHistogramBuilder timeline = AggregationBuilders.dateHistogram("data_over_time");
		if(timelineField == null)
			timeline.field("_timestamp");
		else
			timeline.field(timelineField);
		
		try{
			long intervalAtMilli = Long.parseLong(interval);
			timeline.interval(intervalAtMilli);
		}catch(NumberFormatException e){
			timeline.interval(new Interval(interval));
		}
		
		timelineSearch.addAggregation(timeline);
		
		dumpSearchScriptWhenDebug(timelineSearch);		
		timelineSearch.execute(listener);
	
	}
	
	public void executeReport(final ActionListener<SearchResponse> listener, int from, int size){
		reportSearch.setSearchType(SearchType.COUNT);
		dumpSearchScriptWhenDebug(reportSearch);		
		reportSearch.execute(listener);
	
	}
	
	private void dumpSearchScriptWhenDebug(SearchRequestBuilder search){
		if(logger.isDebugEnabled()){
			
			try {
				XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);			
				search.internalBuilder().toXContent(builder, ToXContent.EMPTY_PARAMS);
				logger.debug( builder.bytes().toUtf8() );
			
			} catch (IOException e) {
				logger.debug(e.getMessage());
			}
		}
	}
	
	public void executeDelete(ActionListener<DeleteByQueryResponse> listener){		
		deleteSearch.execute(listener);
	}



	public void executeDownload(final OutputStream httpStream) {
		
		final SearchResponse head = querySearch.setSearchType(SearchType.SCAN)
			.setSize(200)
			.setScroll(TimeValue.timeValueMinutes(10))
            .execute().actionGet();

		dumpSearchScriptWhenDebug(querySearch);		
		
		new Thread(new Runnable(){

			@Override
			public void run() {
				
				XContentBuilder builder;
				ClearScrollRequestBuilder clear = client.prepareClearScroll();
				try {
					builder = new XContentBuilder(XContentFactory.xContent(XContentType.JSON), httpStream);
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
		
        
        return ;
		
	
	}
	
	public static TermsBuilder newTerms(String name, int size, String[] fields){
		TermsBuilder agg = AggregationBuilders.terms(name).size(size);
		if(fields.length == 1)
			agg.field(fields[0]);
		else
			agg.script(Field.fieldsToScript(fields));
		
		return agg;
	}

}
