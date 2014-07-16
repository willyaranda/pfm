package com.willy.pfm.stream;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableExistsException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
import org.apache.hadoop.hbase.util.Bytes;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

public class SaveTweetBolt extends BaseRichBolt {

	private static final long serialVersionUID = 3961290841705773025L;
	HTable table;
	final String TWEET_TABLE_NAME = "Tweet";
	final String[] TWEET_TABLE_COLUMNS = { "text", "userId", "userName",
			"isRetweet" };

	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		Configuration hc = HBaseConfiguration.create();
		hc.set("hbase.zookeeper.quorum", "54.73.134.90,54.220.28.67,54.216.40.161");
		hc.set("hbase.zookeeper.property.clientPort", "2181");
		hc.set("hbase.master", "54.73.137.63:60000");
		HBaseAdmin hba;
		try {
			table = new HTable(HBaseConfiguration.create(), TWEET_TABLE_NAME);
			hba = new HBaseAdmin(hc);
			if (!hba.tableExists(table.getName())) {
				HTableDescriptor ht = new HTableDescriptor(table.getName());
				for (int i = 0; i < TWEET_TABLE_COLUMNS.length; i++) {
					String column = TWEET_TABLE_COLUMNS[i];
					ht.addFamily(new HColumnDescriptor(column));
				}
				try {
					hba.createTable(ht);
				} catch (TableExistsException tbe) {
					// Do nothing. We have checked early, created late, and
					// other thread might
					// have created the table. Don't worry, be happy.
				}
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public void execute(Tuple tuple) {
		Long tweetId = tuple
				.getLongByField(com.willy.pfm.stream.Fields.TWEET_ID);
		String text = tuple
				.getStringByField(com.willy.pfm.stream.Fields.TWEET_TEXT);
		Long userId = tuple
				.getLongByField(com.willy.pfm.stream.Fields.TWEET_USER_ID);
		String userName = tuple
				.getStringByField(com.willy.pfm.stream.Fields.TWEET_USER_NAME);
		Boolean isRetweet = tuple
				.getBooleanByField(com.willy.pfm.stream.Fields.TWEET_IS_RETWEET);

		// Create a new Row, with the id as string
		Put p = new Put(Bytes.toBytes(tweetId.toString()));

		p.add(Bytes.toBytes("text"), Bytes.toBytes("text"), Bytes.toBytes(text));
		p.add(Bytes.toBytes("userId"), Bytes.toBytes("userId"),
				Bytes.toBytes(userId.toString()));
		p.add(Bytes.toBytes("userName"), Bytes.toBytes("userName"),
				Bytes.toBytes(userName));
		p.add(Bytes.toBytes("isRetweet"), Bytes.toBytes("isRetweet"),
				Bytes.toBytes(isRetweet.toString()));

		try {
			table.put(p);
		} catch (RetriesExhaustedWithDetailsException | InterruptedIOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub

	}
}
