package com.everdata.command;



import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.elasticsearch.action.ActionListener;
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
import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.search.SearchHit;







import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.AbstractAggregationBuilder;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.filter.Filter;
import org.elasticsearch.search.aggregations.bucket.filter.FilterAggregationBuilder;




import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms.Order;
import org.elasticsearch.search.aggregations.metrics.MetricsAggregation.MultiValue;
import org.elasticsearch.search.aggregations.metrics.MetricsAggregation.SingleValue;
import org.elasticsearch.search.sort.SortOrder;

import com.everdata.parser.AST_Search;
import com.everdata.parser.AST_Sort;
import com.everdata.parser.AST_Stats;
import com.everdata.parser.AST_Top;
import com.everdata.parser.CommandParser;
import com.everdata.parser.Node;



public class Search {

	
	ESLogger logger;
	Client client;
	String starttime, endtime;
	SearchRequestBuilder querySearch, reportSearch;
	DeleteByQueryRequestBuilder deleteSearch;
	
	public ArrayList<String> bucketFields = new ArrayList<String>();
	public ArrayList<String> funcFields = new ArrayList<String>();
	
	public Function countField = null;

	public Search(CommandParser parser, Client client, ESLogger logger) throws CommandException{
		
		this.client = client;
		this.logger = logger;
				
		String[] indices = Strings.EMPTY_ARRAY;
		String[] sourceTypes = Strings.EMPTY_ARRAY;
		
		ArrayList<Node> searchCommands = parser.getSearchCommandList();
		
		//search rolling out
		AST_Search searchTree = (AST_Search) searchCommands.get(0);		
		
		indices = searchTree.getOption(Option.INDEX).split(",");
		
		if(searchTree.getOption(Option.SOURCETYPE) != null)
			sourceTypes = searchTree.getOption(Option.SOURCETYPE).split(",");
				
		this.querySearch = client.prepareSearch(indices).setTypes(sourceTypes)
				.setQuery(searchTree.getQueryBuilder());
		this.reportSearch = client.prepareSearch(indices).setTypes(sourceTypes);
		this.deleteSearch = client.prepareDeleteByQuery(indices).setTypes(sourceTypes)
				.setQuery(searchTree.getQueryBuilder());
		
		
		for(int i = 1; i < searchCommands.size(); i++){
			
			if (searchCommands.get(i) instanceof AST_Sort){
				AST_Sort sortTree = (AST_Sort) 	searchCommands.get(i);
				
				for(String field: sortTree.sortFields){
					
					if(field.startsWith("+")){
						querySearch.addSort(field.substring(1), SortOrder.ASC);
					}else if(field.startsWith("-")){
						querySearch.addSort(field.substring(1), SortOrder.DESC);
					}else
						querySearch.addSort(field, SortOrder.ASC);
				}

			}
		}
		
		ArrayList<Node> reportCommands = parser.getReportCommandList();
		
		if(reportCommands.size() > 0){
			
			FilterBuilder filter = ( searchTree.getInternalFilter() == null )? FilterBuilders.matchAllFilter() : searchTree.getInternalFilter();
			
			FilterAggregationBuilder report = AggregationBuilders.filter("report").filter(filter);
			
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
					
					countField = ((AST_Stats) child).count;
					
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
				report.subAggregation(stat);
			}
			
			reportSearch.addAggregation(report);
			
		}
		
		
		
		
		
	}
	public void buildQuery(int from, XContentBuilder builder, SearchResponse response)
			throws IOException {

		HashSet<String> fields = new HashSet<String>();

		SearchHits hits = response.getHits();
		
		logger.debug("Query took in millseconds:" + response.getTookInMillis());

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
		
		builder.startArray("fields");
		for (String key : fields) {
			builder.value( key);
		}
		builder.endArray();
		
		

		builder.startArray("rows");
		iterator = hits.iterator();
		while (iterator.hasNext()) {
			hit = iterator.next();
			builder.startArray();
			for (String key : fields) {							
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
	public void buildReport(int from, XContentBuilder builder,
			SearchResponse response) throws IOException, CommandException {

		logger.debug("Report took in millseconds:" + response.getTookInMillis());
		
		List<String> fields = new ArrayList<String>();
		List<List<String>> rows = new ArrayList<List<String>>();

		Filter report = (Filter) response.getAggregations().get("report");
		
		//构建表头
		fields.addAll(bucketFields);
		fields.addAll(funcFields);
		if(countField != null){
			fields.add(Function.genStatField(countField));
		}
		
		fields.add("_count");
		
		
		if (report.getAggregations().get("statsWithBy") != null) {
			// 一级结构
			Terms terms = report.getAggregations().get("statsWithBy");

			Iterator<Terms.Bucket> iterator = terms.getBuckets().iterator();
			while (iterator.hasNext()) {
				List<String> row = new ArrayList<String>();
				
				Terms.Bucket next = iterator.next();

				for (String value : next.getKey().split("\\|"))
					// 分组Key的具体值，e.g: www.sina.com, 多值 www.sina.com|80|123
					row.add(value);

				Map<String, Aggregation> map = next.getAggregations()
						.getAsMap();


				for (String k : funcFields) {
					Aggregation agg = map.get(k);

					if (agg instanceof SingleValue) {
						double value = ((SingleValue) agg).value();
						row.add(String.valueOf(value));
					} else if (agg instanceof MultiValue) {
						throw new CommandException("暂不支持 MultiValue");
					}

				}
				
				if(countField != null){
					row.add(String.valueOf(next.getDocCount()));
				}
				
				row.add(String.valueOf(next.getDocCount()));
				
				rows.add(row);
			}

		} else if (report.getAggregations().get("topWithBy") != null) {
			// 两级bucket结构
			
			Terms terms = report.getAggregations().get("topWithBy");

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
		} else if (report.getAggregations().get("top") != null) {
			// 一级结构
			Terms terms = report.getAggregations().get("top");

			Iterator<Terms.Bucket> firstIterator = terms.getBuckets().iterator();
			while (firstIterator.hasNext()) {
				List<String> row = new ArrayList<String>();
				
				Terms.Bucket firstBucket = firstIterator.next();

				for (String value : firstBucket.getKey().split("\\|"))
						// 分组Key的具体值，e.g: www.sina.com, 多值 www.sina.com|80|123
						row.add(value);
				
				if(countField != null){
					row.add(String.valueOf(firstBucket.getDocCount()));
				}
				row.add(String.valueOf(firstBucket.getDocCount()));
				rows.add(row);
								
			}
		} else {
			// 0级结构
			List<String> row = new ArrayList<String>();
			
			for (String k : funcFields) 
				row.add(String.valueOf(((SingleValue)report.getAggregations().get(k)).value()));
			
			if(countField != null){
				row.add(String.valueOf(report.getDocCount()));
			}
			row.add(String.valueOf(report.getDocCount()));
			
			rows.add(row);
		}
		
		

		builder.startObject();
		builder.field("took", response.getTookInMillis());
		builder.field("total", rows.size());
		builder.field("from", from);
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
	
	public void executeQuery(final ActionListener<SearchResponse> listener, int from, int size) {
				

		querySearch.setSearchType(SearchType.QUERY_THEN_FETCH)
				.setFrom(from)
				.setSize(size);
		dumpSearchScriptWhenDebug(querySearch);
		
		querySearch.execute(listener);

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

}
