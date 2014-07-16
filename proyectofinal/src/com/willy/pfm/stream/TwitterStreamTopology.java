package com.willy.pfm.stream;

import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

public class TwitterStreamTopology implements Serializable {
	private static final long serialVersionUID = 4427572778233130276L;

	private TopologyBuilder builder = new TopologyBuilder();
	private static Config conf = new Config();
	private LocalCluster cluster;

	public TwitterStreamTopology() throws IOException {
		// Spout to read all tweets
		builder.setSpout("streamTweet", new StreamTweetsSpout(), 1);

		// We early-analyze things here: stream users and then tweets
		builder.setBolt("analyzeUser", new AnalyzeUserBolt(), 10)
				.shuffleGrouping("streamTweet");
		builder.setBolt("analyzeTweet", new AnalyzeTwitBolt(), 10)
				.shuffleGrouping("streamTweet");

		// Al usar .fieldsGrouping, todo lo igual ir� al mismo SaveUserBolt,
		// por lo que pasamos de distribuir de una forma uniforme pero aleatoria
		// los datos a que el sistema nos distribuya un mismo usuario a un mismo
		// bolt
		// haciendo que no enviemos usuarios duplicados.
		builder.setBolt("saveUser", new SaveUserBolt(), 10).fieldsGrouping(
				"analyzeUser",
				new Fields(com.willy.pfm.stream.Fields.TWEET_USER_ID));

		builder.setBolt("saveTweet", new SaveTweetBolt(), 10).shuffleGrouping(
				"analyzeTweet");
	}

	public TopologyBuilder getBuilder() {
		return builder;
	}

	public LocalCluster getLocalCluster() {
		return cluster;
	}

	public Config getConf() {
		return conf;
	}

	public void runLocal(int runTime) {
		conf.setDebug(false);
		conf.put(Config.STORM_ZOOKEEPER_SERVERS, Arrays.asList("54.73.134.90", "54.220.28.67","54.216.40.161"));
		cluster = new LocalCluster();
		cluster.submitTopology("StreamTweetTopology", conf,
				builder.createTopology());
		/*if (runTime > 0) {
			Utils.sleep(runTime);
			shutDownLocal();
		}*/
	}

	public void shutDownLocal() {
		if (cluster != null) {
			cluster.killTopology("TwitterStreamTopology");
			cluster.shutdown();
		}
	}

	public void runCluster(String name) throws AlreadyAliveException,
			InvalidTopologyException {
		conf.setNumWorkers(20);
		StormSubmitter.submitTopology(name, conf, builder.createTopology());
	}

	public static void main(String[] args) throws Exception {
		TwitterStreamTopology topology = new TwitterStreamTopology();
		// topology.runCluster("twitteame");
		topology.runLocal(10000);
	}
}
