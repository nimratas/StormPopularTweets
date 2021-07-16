package StormPA2.PA2;

import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import twitter4j.*;

import twitter4j.conf.ConfigurationBuilder;

@SuppressWarnings("serial")
public class TwitterStreamSpout extends BaseRichSpout {

	public SpoutOutputCollector _collector;
	private LinkedBlockingQueue<Status> queue = null;

	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		_collector = collector;
		queue = new LinkedBlockingQueue<Status>(500);

		StatusListener listener = new StatusListener() {
			
			public void onException(Exception ex) {
				ex.printStackTrace();
			}
			
			public void onTrackLimitationNotice(int numberOfLimitedStatuses) {}
			
			public void onStatus(twitter4j.Status status) {
				queue.offer(status);
			}
			
			public void onStallWarning(StallWarning warning) {}
			
			public void onScrubGeo(long userId, long upToStatusId) {}
			
			public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {}
		};
		ConfigurationBuilder cb = new ConfigurationBuilder();
		cb.setDebugEnabled(true)
		.setOAuthConsumerKey("L5itw5EwAkNmZjcX2JCsOy3UK")
		.setOAuthConsumerSecret("08FFTxXEFCEI6Rl1EASHcljBk3Uqkrmys2mgefyyURKp2Bo7UW")
		.setOAuthAccessToken("925177388719161344-0Me15Fx4G0qKgfdmbSbv2BdjCZSQ1X6")
		.setOAuthAccessTokenSecret("Pc732CKEGFcD0iTswkRBABA4B2AKSNmpddhayfrtjK252");

		TwitterStream twitterStream = new TwitterStreamFactory(cb.build()).getInstance();
		twitterStream.addListener(listener);
		twitterStream.sample("en");
	}

	public void nextTuple() {
		Status status = queue.poll();
		if(status == null) {
			Utils.sleep(50);
			
		}
		else {
			_collector.emit(new Values(status));
		}
	}
	
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("tweet"));// emit tweets into the topology
	}
}


