package com.willy.pfm.stream;

import java.util.Map;

import twitter4j.Status;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class AnalyzeUserBolt extends BaseRichBolt {
	/**
	 * 
	 */
	private static final long serialVersionUID = 3961290841705773023L;
	private OutputCollector collector;

	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this.collector = collector;
	}

	@Override
	public void execute(Tuple tuple) {
		Status status = (Status) tuple
				.getValueByField(com.willy.pfm.stream.Fields.TWEET_FULL);
		String screenName;
		Long id;
		screenName = status.getUser().getScreenName();
		id = status.getUser().getId();
		collector.emit(new Values(screenName, id));
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields(
				com.willy.pfm.stream.Fields.TWEET_USER_NAME,
				com.willy.pfm.stream.Fields.TWEET_USER_ID));
	}
}
