package StormPA2.PA2;

import java.io.File;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import edu.stanford.nlp.ling.CoreAnnotations.SentencesAnnotation;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations;
import edu.stanford.nlp.util.CoreMap;
import twitter4j.Status;


public class SentiScoreBolt extends BaseBasicBolt {
	public OutputCollector collector;
	PrintWriter writer;
	int sentiScore = 0;

	public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		try {
			File writeFile = new File("/s/chopin/k/grad/nimrata/SentiTweets.txt");
			writer = new PrintWriter(writeFile);
		} catch (Exception e) {
			System.out.println("UNABLE TO WRITE FILE :: 1 ");
			e.printStackTrace();
		}
	}

	public void execute(Tuple input, BasicOutputCollector collector) {
		Status tweet = (Status) input.getValueByField("tweet");
		String tweets = tweet.getText();
		int sentiValue = 0;
		Properties properties = new Properties();
		properties.setProperty("annotators", "tokenize, ssplit, pos, parse, sentiment");
		StanfordCoreNLP pipeline = new StanfordCoreNLP(properties);
		Annotation document = new Annotation(tweets);
		pipeline.annotate(document);

		List<CoreMap> sentences = document.get(SentencesAnnotation.class);
		for (CoreMap sentence : sentences) {
			String sentiment = sentence.get(SentimentCoreAnnotations.SentimentClass.class);
			if (sentiment.equalsIgnoreCase("Neutral")) {
				sentiValue = 0;
			}
			if (sentiment.equalsIgnoreCase("Very positive")) {
				sentiValue = 2;
			}
			if (sentiment.equalsIgnoreCase("Positive")) {
				sentiValue = 1;
			}
			if (sentiment.equalsIgnoreCase("Negative")) {
				sentiValue = -1;
			}
			if (sentiment.equalsIgnoreCase("Very negative")) {
				sentiValue = -2;
			}
		}
		collector.emit(new Values(tweet, sentiValue));
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("Tweet", "sentiValue"));
	}

	public void cleanup() {
		writer.close();
		super.cleanup();

	}

}
