package StormPA2.PA2;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.List;
import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import twitter4j.HashtagEntity;
import twitter4j.Status;

public class HashtagBolt extends BaseRichBolt {
	private OutputCollector _collector;
	private int count = 0;
	private PrintWriter writer;

	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this._collector = collector;
		try {
			writer = new PrintWriter(new File("/s/chopin/k/grad/nimrata/storm/apache-storm-1.1.1/hashtag.txt"));
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
	}

	public void execute(Tuple input) {
		Status tweet = (Status) input.getValueByField("Tweet");
		int sentiScore = (Integer) input.getValueByField("sentiValue");
		_collector.ack(input);
		for(HashtagEntity hashtag : tweet.getHashtagEntities()) {
			String hash = hashtag.getText().toString();
			if(!hash.isEmpty()) {
				System.out.println("HashTag : ----- "+ hash);
				_collector.emit(new Values(hash, sentiScore));
			}
		}
		
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {

		declarer.declare(new Fields("hashtags", "sentiScore"));
	}
	

}
