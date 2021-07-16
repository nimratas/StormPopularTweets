package StormPA2.PA2;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.Collections;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.storm.shade.org.apache.commons.lang.StringUtils;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class PrinterBolt extends BaseBasicBolt {
	private OutputCollector _collector;
	private PrintWriter writer;
	private int count = 0;

	public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
		_collector = outputCollector;
	}

	public void execute(Tuple input, BasicOutputCollector collector) {
		String text = input.getStringByField("message");

		StringTokenizer tokenizer = new StringTokenizer(text);
		System.out.println("-------------------------Split by # ------------------------ ");
		while(tokenizer.hasMoreElements()) {
			String term = (String)tokenizer.nextElement();
			if(StringUtils.startsWith(term, "#"));
			_collector.emit(new Values(term));
			System.out.println("------------------HashTag-----------" + term);
		}
	}	

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("Hashtag"));
	}

	public void cleanup() {
		super.cleanup();
	}
}
