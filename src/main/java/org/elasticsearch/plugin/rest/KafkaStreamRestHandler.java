package org.elasticsearch.plugin.rest;

import static org.elasticsearch.rest.RestRequest.Method.GET;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestStatus;
import org.apache.spark.SparkConf;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;

public class KafkaStreamRestHandler extends BaseRestHandler {
	
	static class KafkaStreamSeqOutputFormat extends SequenceFileOutputFormat <Text,BytesWritable>{
		
	}
	
	private ConcurrentHashMap<String, JavaStreamingContext> topicContextMap = new ConcurrentHashMap<String, JavaStreamingContext>();

	@Inject
	public KafkaStreamRestHandler(Settings settings,
			RestController controller, Client client) {
		super(settings, controller, client);
		controller.registerHandler(GET, "/_datastore", this);
	}

	@Override
	protected void handleRequest(RestRequest request, RestChannel channel, Client client)
			throws Exception {
		final String topic = request.param("topic", "");
		final boolean schema = request.paramAsBoolean("schema", false);
		final String master = request.param("masterAddress", "local");
		final String hdfs =  request.param("hdfs", "hdfs://localhost:50070");
		final String memory =  request.param("memory", "2g");
		final String appName = request.param("appName", "appName-"+topic);
		final int duration = request.paramAsInt("duration", 1000);
		
		Thread exec = new Thread(new Runnable(){

			@Override
			public void run() {
			
				SparkConf sparkConf = new SparkConf().setAppName(appName).setMaster(master).set("spark.executor.memory", memory);
				JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, new Duration(duration));
				
				Map<String, Integer> topicMap = new HashMap<String, Integer>();
				topicMap.put(topic, 3);
				
				JavaPairReceiverInputDStream<String, byte[]> kafkaStream = KafkaUtils.createStream(jssc, String.class, byte[].class, 
							kafka.serializer.DefaultDecoder.class, kafka.serializer.DefaultDecoder.class, null, 
							topicMap,  StorageLevel.MEMORY_ONLY());
		
				//JobConf confHadoop = new JobConf();
				//confHadoop.set("mapred.output.compress", "true");
				//confHadoop.set("mapred.output.compression.codec", "com.hadoop.compression.lzo.LzopCodec");
		
				kafkaStream.saveAsHadoopFiles(hdfs, "seq", Text.class, BytesWritable.class, KafkaStreamSeqOutputFormat.class);
				
				topicContextMap.put(topic, jssc);
				jssc.start();		
				jssc.awaitTermination();
				
			}
		});
		
		exec.start();
		
		channel.sendResponse(new BytesRestResponse(RestStatus.OK, String.format("{\"topic\":\"%s\"}",  topic)));
		
		
	}

}
