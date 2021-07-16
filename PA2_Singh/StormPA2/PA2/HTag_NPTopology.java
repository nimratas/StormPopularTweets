package StormPA2.PA2;

import org.apache.log4j.Logger;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

public class HTag_NPTopology  {
	private static final String TWITTER_SPOUT_ID = "TwitterStreamSpout";
	private static final String HASHTAG_BOLT = "PrinterBolt";
	private static final String TOPOLOGY_NAME = "TwitterPA2-Topology";

	private static final Logger LOG = Logger.getLogger(Topology.class);
	private static final int DEFAULT_RUNTIME_IN_SECONDS = 60;
	private static final int TOP_N = 5;
	private static final long MILLIS_IN_SEC = 10000000;

	private final TopologyBuilder builder;
	private final String topologyName;
	private final Config topologyConfig;
	private final int runtimeInSeconds;

	public HTag_NPTopology(String topologyName) throws InterruptedException {
		builder = new TopologyBuilder();
		this.topologyName = topologyName;
		topologyConfig = createTopologyConfiguration();
		runtimeInSeconds = DEFAULT_RUNTIME_IN_SECONDS;

		wireTopology();
	}

	private static Config createTopologyConfiguration() {
		Config conf = new Config();
		conf.setDebug(true);
		return conf;
	}

	private void wireTopology() throws InterruptedException {
		String spoutId = "TwitterStreamSpout";
		String boltId = "SentiScoreBolt";

		builder.setSpout(spoutId, new TwitterStreamSpout());
		builder.setBolt("SentiScoreBolt", new SentiScoreBolt()).shuffleGrouping(spoutId);
		builder.setBolt("HashTagBolt", new HashtagBolt()).shuffleGrouping(boltId);
		builder.setBolt("LossyBucketBolt", new LossyBucketBolt()).fieldsGrouping("HashTagBolt", new Fields("hashtags", "sentiScore"));
		builder.setBolt("FileWriterBolt", new FileWriterBolt()).globalGrouping("LossyBucketBolt");

	}

	public void runLocally() throws InterruptedException {
		runTopologyLocally(builder.createTopology(), topologyName, topologyConfig, runtimeInSeconds);
	}

	public void runRemotely() throws Exception {
		runTopologyRemotely(builder.createTopology(), topologyName, topologyConfig);
	}

	public static void runTopologyLocally(StormTopology topology, String topologyName, Config conf, int runtimeInSeconds)
			throws InterruptedException {
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology(topologyName, conf, topology);

	}
	public static void runTopologyRemotely(StormTopology topology, String topologyName, Config conf)
			throws AlreadyAliveException, InvalidTopologyException, AuthorizationException {
		StormSubmitter.submitTopology(topologyName, conf, topology);
	}

	public static void main(String[] args) throws Exception   {

		String topologyName = "TwitterTopology";
		if (args.length >= 1) {
			topologyName = args[0];
		}
		boolean runLocally = true;
		if (args.length >= 2 && args[1].equalsIgnoreCase("remote")) {
			runLocally = false;
		}

		LOG.info("Topology name: " + topologyName);
		Topology top = new Topology(topologyName);
		if (runLocally) {
			LOG.info("Running in local mode");
			top.runLocally();
		}
		else {
			LOG.info("Running in remote (cluster) mode");
			top.runRemotely();
		}
	}
}
