package StormPA2.NER;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

public class FileWriterBolt extends BaseRichBolt {
	private double TimeOfStart;
	private PrintWriter writer;
	private double currTime;
	DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
	private HashMap<Double, List<Variables>> counts = null;

	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.counts = new HashMap<Double, List<Variables>>();
		TimeOfStart = new Date().getTime() / 1000;
		try {
			writer = new PrintWriter(new File("/s/chopin/k/grad/nimrata/storm/LogFile1.txt"));
		} catch (FileNotFoundException e) {
			System.out.println("UNABLE TO PRINT FILE 1!!");
			e.printStackTrace();
		}

	}

	public void execute(Tuple input) {
		currTime = new Date().getTime() / 1000;
		String hashtag = input.getStringByField("hashtag");
		int count = input.getIntegerByField("count");
		int sentimentValue = (Integer) input.getValueByField("sentimentValue");
		Variables v = new Variables();
		v.element = hashtag;
		v.freq = count;
		v.sentiment = sentimentValue;
		v.timestamp = currTime;
		if (counts.containsKey(currTime)) {
			List<Variables> list = counts.get(currTime);
			list.add(v);
			counts.put(currTime, list);
		} else {
			List<Variables> list = new ArrayList<Variables>();
			list.add(v);
			counts.put(currTime, list);
		}
		if (currTime - TimeOfStart >= 10) {
			writeLogs();
			TimeOfStart = new Date().getTime() / 1000;
		}
	}

	public void writeLogs() {

		System.out.println("Displays after every 10 seconds" + "\n");

		System.out.println("Start time: " + String.format("%12.0f", TimeOfStart));
		System.out.println("--- DIFFERENCE: " + (currTime - TimeOfStart) + " ---" + String.format("%12.0f", currTime));
		System.out.println("Below are the top hashtags");
		HashMap<String, Variables> counts_mid = new HashMap<String, Variables>();
		List<Double> getkeys = new ArrayList<Double>(counts.keySet());
		Calendar cal = Calendar.getInstance();
		System.out.println(dateFormat.format(cal.getTime()));

		for (double key : getkeys) {
			if (key <= TimeOfStart + 10) {
				List<Variables> variables = counts.get(key);
				for (Variables v : variables) {
					if (counts_mid.get(v.element) != null) {
						if (counts_mid.get(v.element).timestamp < v.timestamp) {
							counts_mid.put(v.element, v);
						}
					} else {
						counts_mid.put(v.element, v);
					}
				}
				counts.remove(key);
			}
		}

		Map<Integer, List<Variables>> entryCount = new TreeMap<Integer, List<Variables>>();
		for (Map.Entry<String, Variables> entry : counts_mid.entrySet()) {
			if (entryCount.get(entry.getValue().freq) != null) {
				List<Variables> oList = entryCount.get(entry.getValue().freq);
				oList.add(entry.getValue());
				entryCount.put(entry.getValue().freq, oList);
			} else {
				List<Variables> vList = new ArrayList<Variables>();
				vList.add(entry.getValue());
				entryCount.put(entry.getValue().freq, vList);
			}
		}

		/*SORTED MAP IS SORTED BASED ON COUNT*/
		List<Integer> sortedkeySet = new ArrayList<Integer>(entryCount.keySet());
		Collections.sort(sortedkeySet, Collections.reverseOrder());

		//_log.write("Time : "+dateFormat.format(cal.getTime()));
		for (Integer keysget : sortedkeySet) {
			for (Variables v : entryCount.get(keysget)) {
				System.out.println(v.element + " : " + v.freq + " --- " + v.freq + "Sentimental Value: " + v.sentiment + "\n");
				writer.write("Time : " + dateFormat.format(cal.getTime()) + "  " + "#" + v.element + "Sentimental Value:" + v.sentiment + "\n");
				writer.flush();
			}
		}
		System.out.println("--------------");
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {}

	public void cleanup() {
		writer.close();
	}
}
