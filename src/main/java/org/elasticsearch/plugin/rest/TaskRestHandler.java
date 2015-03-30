package org.elasticsearch.plugin.rest;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.POST;

import java.io.IOException;
import java.security.InvalidParameterException;
import java.util.ArrayList;



import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SchemaRDD;
import org.apache.spark.sql.api.java.JavaSQLContext;
import org.apache.spark.sql.api.java.JavaSchemaRDD;
import org.apache.spark.storage.StorageLevel;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.cache.Cache;
import org.elasticsearch.common.cache.CacheBuilder;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.hadoop.util.StringUtils;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.spark.sql.api.java.JavaEsSparkSQL;

import com.everdata.command.CommandException;
import com.everdata.command.Search;
import com.everdata.parser.CommandParser;
import com.everdata.parser.ParseException;


public class TaskRestHandler extends BaseRestHandler {
	static enum TaskStatus{
		RUNNING,ERROR,DONE
	}

	static class TaskResponse{
		
		public TaskStatus status = TaskStatus.RUNNING;
		public Exception e = null;
		public String task;
	}
	
	@Inject
	public TaskRestHandler(Settings settings, Client client,
			RestController controller) {
		super(settings, controller, client);
		this.client = client;
		controller.registerHandler(GET, "/_task", this);
		controller.registerHandler(POST, "/_task", this);
		controller.registerHandler(GET, "/_taskstatus/{taskid}", this);
	}
	
	private Client client;
	private static Cache<String, TaskResponse> taskResponse = CacheBuilder.newBuilder()
		       .maximumSize(200)
		       .expireAfterAccess(20, TimeUnit.MINUTES)
		       .build();

	@Override
	public void handleRequest(RestRequest request, RestChannel channel, Client arg2)
			throws Exception {
		
		
		final String taskid;
		
		if( request.param("taskid") != null){
			taskid = request.param("taskid");
			BytesRestResponse bytesRestResponse;
			if(taskResponse.getIfPresent(taskid) == null){
				bytesRestResponse = new BytesRestResponse(RestStatus.OK, String.format("{\"status\":\"%s\",\"message\":\"%s\"}",  "error", "taskid is not exist"));
			}else if(taskResponse.getIfPresent(taskid).status == TaskStatus.ERROR){				
				bytesRestResponse = new BytesRestResponse(RestStatus.OK, String.format("{\"status\":\"%s\",\"message\":\"%s\"}",  "error", taskResponse.getIfPresent(taskid).e.toString()));
			}else if(taskResponse.getIfPresent(taskid).status == TaskStatus.RUNNING){
				bytesRestResponse = new BytesRestResponse(RestStatus.OK, String.format("{\"status\":\"%s\"}",  "running"));			
			}else{
				bytesRestResponse = new BytesRestResponse(RestStatus.OK, String.format("{\"status\":\"%s\",\"message\":\"%s\"}",  "done", taskResponse.getIfPresent(taskid).task));
			}
			
			channel.sendResponse(bytesRestResponse);
			return;
		}
		
		taskid = java.util.UUID.randomUUID().toString();
		
		final String esTable = request.param("esTable", "");
		final String esTable2 = request.param("esTable2", "");
		
		final String parTable = request.param("parTable", "");
		final String parTable2 = request.param("parTable2", "");
		
		final String resultIndex = request.param("targetIndex", "index-tasks");
		final String resultType = request.param("targetType", "type-"+taskid);
		
		final String targetPar = request.param("targetPar");
		
		final String appName = request.param("appName", "appName-"+taskid);
		
		final String master = request.param("masterAddress", "local");
		
		final String sql = request.param("sql", "");
		
		final String memory =  request.param("memory", "2g");
		final String resultTable = String.format("%s/%s", resultIndex, resultType);
		//ES_RESOURCE_READ -> resource, ES_QUERY -> query
        final HashMap<String, String> cfg = new HashMap<String, String>();
        
        cfg.put(org.elasticsearch.hadoop.cfg.ConfigurationOptions.ES_RESOURCE_WRITE, resultTable);
        ClusterState cs = client.admin().cluster().prepareState().execute()
				.actionGet().getState();
        ArrayList<String> nodesAddr = new ArrayList<String>();
        for(DiscoveryNode node :cs.nodes()){
        	nodesAddr.add(node.getHostAddress());
        }
        String esNodes = StringUtils.concatenate( nodesAddr,",");
        logger.info(esNodes);
        cfg.put(org.elasticsearch.hadoop.cfg.ConfigurationOptions.ES_NODES, esNodes);
		
		taskResponse.put(taskid, new TaskResponse());
		
		Thread exec = new Thread(new Runnable(){

			@Override
			public void run() {
				try{
					executeSparkSql( sql,  esTable,  esTable2,
							 parTable,  parTable2, 
							 targetPar, resultTable, appName,	 master	, memory, cfg 		
							);
					
					taskResponse.getIfPresent(taskid).status = TaskStatus.DONE;
					if(targetPar == null)
						taskResponse.getIfPresent(taskid).task = resultTable;
					else
						taskResponse.getIfPresent(taskid).task = targetPar;
					
				}catch(Exception e){
					taskResponse.getIfPresent(taskid).status = TaskStatus.ERROR;
					taskResponse.getIfPresent(taskid).e = e;
					logger.error("executeSparkSql", e);
					
				}
				
			}
			
		});
		
		exec.start();

				
		channel.sendResponse(new BytesRestResponse(RestStatus.OK, String.format("{\"taskid\":\"%s\"}",  taskid)));
		
		
	}
	
	static private void registerAndUnionTable(List<SchemaRDD> rdds, String tableName ){
		SchemaRDD wholeRdd = rdds.get(0);		
		
		for(int idx = 1; idx < rdds.size(); idx++){
			wholeRdd = wholeRdd.unionAll(rdds.get(idx));
    	}
		
		wholeRdd.registerTempTable(tableName);		
	}
	
	final static private String COMMA = ";";
	
	private void executeSparkSql(String sql, String esTable, String esTable2,
			String parTable, String parTable2,
			String targetPar, String resultTable, String appName,	String master, String memory, Map<String, String> cfg
			) throws ParseException, CommandException, IOException{
		
		if( sql.length() <= 0 )
			throw new InvalidParameterException("sql String is null");
		
		SparkConf sparkConf = new SparkConf().setAppName(appName).setMaster(master).set("spark.executor.memory", memory);
		
        JavaSparkContext ctx = new JavaSparkContext(sparkConf);
        
        try{
	        JavaSQLContext sqlContext = new JavaSQLContext(ctx);
	        
	        if(parTable.length() > 0){
	        	String[] parTables = parTable.split(COMMA);
	        	
	        	List<SchemaRDD> rdds = new ArrayList<SchemaRDD>();
	        	
	        	for(String parTablePath: parTables){
	        		rdds.add(sqlContext.parquetFile(parTablePath).baseSchemaRDD());
	        	}
	        	
	        	registerAndUnionTable(rdds, "parTable");
	        }
	
	        if(parTable2.length() > 0){
	        	String[] parTables = parTable.split(COMMA);
	        	
	        	List<SchemaRDD> rdds = new ArrayList<SchemaRDD>();
	        	
	        	for(String parTablePath: parTables){
	        		rdds.add(sqlContext.parquetFile(parTablePath).baseSchemaRDD());
	        	}
	        	registerAndUnionTable(rdds, "parTable2");	        	
	        }
	        
	        if(esTable.length() > 0){
	        	getSchemaRDD(sqlContext, Search.PREFIX_SEARCH_STRING + " " + esTable).registerTempTable("esTable");
	        }
	        
	        if(esTable2.length() > 0){
	        	getSchemaRDD(sqlContext, Search.PREFIX_SEARCH_STRING + " " + esTable2).registerTempTable("esTable2");        }
	        
	        JavaSchemaRDD results = sqlContext.sql(sql);
	        
	        if(targetPar == null)	        
	        	JavaEsSparkSQL.saveToEs(results, cfg);	        
	        else
	        	results.saveAsParquetFile(targetPar);
	        
        }catch(Exception e){
        	logger.error("error", e);        	
        	throw e;
        }finally{
        	ctx.stop();
        }
        
        
	} 
	
	private JavaSchemaRDD  getSchemaRDD(JavaSQLContext sqlContext, String command) throws ParseException, CommandException, IOException{
		CommandParser parser = new CommandParser(command);

		Search search = new Search(parser, client, logger);
		
		if(search.indices.length == 0 || search.indices.length > 1)
			throw new InvalidParameterException(String.format("indices.length = %d", search.indices.length));
		
		if(search.sourceTypes.length == 0 || search.sourceTypes.length > 1)
			throw new InvalidParameterException(String.format("sourceTypes.length = %d", search.sourceTypes.length));
		
		String query = search.querySearch.toString();
		
		return JavaEsSparkSQL.esRDD(sqlContext, String.format("%s/%s", search.indices[0], search.sourceTypes[0]), query);
	}

}
