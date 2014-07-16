package com.willy.pfm.stream;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Map;

import twitter4j.FilterQuery;
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

public final class StreamTweetsSpout extends BaseRichSpout implements
		Serializable, StatusListener {

	private static final long serialVersionUID = 3911371302098646756L;
	private SpoutOutputCollector collector;
	private TwitterStream twitterStream;
	private FilterQuery query;
	private String[] KEYWORDS = { "Justin Bieber", "Bieber", "@justinbieber",
			"believers", "believer", "#beliebers" };
	
	private Integer counter = 0;
	
	private ArrayList<Status> statuses = new ArrayList<Status>();

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields(com.willy.pfm.stream.Fields.TWEET_FULL));
	}

	@Override
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		/**
		 * Initialize twitter library and filter keywords
		 */
		twitterStream = new TwitterStreamFactory().getInstance();
		twitterStream.addListener(this);
		query = new FilterQuery().track(KEYWORDS);
		twitterStream.filter(query);

		this.collector = collector;
	}

	@Override
	synchronized public void nextTuple() {
		if (statuses.isEmpty()) {
			Utils.sleep(100);
		}
		for (Status status : statuses) {
			collector.emit(new Values(status));
		}
		statuses.clear();
	}

	@Override
	public void onException(Exception arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void onDeletionNotice(StatusDeletionNotice arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void onScrubGeo(long arg0, long arg1) {
		// TODO Auto-generated method stub

	}

	@Override
	public void onStallWarning(StallWarning arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	synchronized public void onStatus(Status status) {
		if ((++counter % 100) == 0) {
			System.out.println("Received " + counter + " tweets");
		}
		statuses.add(status);
	}

	@Override
	public void onTrackLimitationNotice(int arg0) {
		// TODO Auto-generated method stub

	}
}
