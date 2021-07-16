package StormPA2.PA2;

import java.util.Map;

import org.apache.storm.shade.org.jboss.netty.util.internal.ConcurrentHashMap;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class LossyBucketBolt extends BaseRichBolt {
	int sentiment = 0;
	private OutputCollector collector;
	private int num_elements=0;
	private double e=0.05f;
	private int current_bucket=1;
	private int bucket_size=(int) Math.ceil(1/e);
	private Map<String, Variables> window = new ConcurrentHashMap<String,Variables>();

	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
	}

	public void execute(Tuple input) {
		String hashtag = input.getStringByField("hashtags");
		sentiment = (Integer) input.getValueByField("sentiScore");
		lossyBucket(hashtag);
	}
	
	public void lossyBucket(String hashtag) {
		if(num_elements<bucket_size) {
			if(!window.containsKey(hashtag)) {
				Variables v = new Variables();
				v.element = hashtag;
				v.freq = 1;
				v.delta = current_bucket-1;
				v.sentiment = sentiment;
				window.put(hashtag, v);
			}
			else {
				Variables v = window.get(hashtag);
				v.freq += 1;
				window.put(hashtag, v);
				}
			num_elements += 1;
		
		}
		if(num_elements == bucket_size) {
			deletePhase();
			for(String hshtag: window.keySet()) {
				Variables v = window.get(hshtag);
				collector.emit(new Values(v.element, v.freq, v.sentiment));
			}
		}
	}
	public void deletePhase() {
		for(String hashtag : window.keySet()) {
			Variables v = window.get(hashtag);
			double freqSumDelta = v.freq + v.delta;
			if(freqSumDelta <= current_bucket) {
				window.remove(hashtag);
			}
		}
	}
	
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("hashtag", "count", "sentimentValue"));
	}

}
