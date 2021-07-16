package StormPA2.NER;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;

import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;

import edu.stanford.nlp.ie.AbstractSequenceClassifier;
import edu.stanford.nlp.ie.crf.CRFClassifier;
import edu.stanford.nlp.ling.CoreLabel;
import twitter4j.Status;

public class NERBolt extends BaseRichBolt {
	private OutputCollector _collector;
    private PrintWriter writer ;
    private String serializedClassifier = null;


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
		serializedClassifier = "edu/stanford/nlp/models/ner/english.muc.7class.distsim.crf.ser.gz";
		AbstractSequenceClassifier<CoreLabel> classifier = null;
		try {
			classifier = CRFClassifier.getClassifier(serializedClassifier);
			
		} catch (ClassCastException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		String term;
		
	}
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("entities", "sentiScore"));
	}

}
