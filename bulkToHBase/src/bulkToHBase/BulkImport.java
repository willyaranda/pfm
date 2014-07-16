package bulkToHBase;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.protobuf.ServiceException;

public class BulkImport {
	private final static String USER_TABLE_NAME = "User";
	private final static String TWEET_TABLE_NAME = "Tweet";
	
	public static void main(String[] args) throws IOException {
		
		ArrayList<Put> putsUsers = new ArrayList<>(1000);
		ArrayList<Put> putsTweets = new ArrayList<>(1000);

		Configuration config = HBaseConfiguration.create();
		config.set("hbase.zookeeper.quorum", "54.73.134.90,54.220.28.67,54.216.40.161");
		config.set("hbase.zookeeper.property.clientPort", "2181");
		config.set("hbase.master", "54.73.137.63:60000");
		
		String csvFile = "/Users/willyaranda/Desktop/tweets/tweet-uniq-orig.csv";
		BufferedReader br = null;
		String line = "";
		String cvsSplitBy = "\t";
		try {
			HBaseAdmin.checkHBaseAvailable(config);
		} catch (ServiceException | IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		HTable userTable = new HTable(config, USER_TABLE_NAME);
		HTable tweetTable = new HTable(config, TWEET_TABLE_NAME);
		
		Integer counter = 0;
		
		try {
			br = new BufferedReader(new FileReader(csvFile));
			while ((line = br.readLine()) != null) {
				counter++;
				if (counter % 1000 == 0) {
					System.out.println("Going to put " + counter);
					tweetTable.put(putsTweets);
					userTable.put(putsUsers);
					putsTweets.clear();
					putsUsers.clear();
				}
				if (counter < 2520000) continue;
				String[] data = line.split(cvsSplitBy);
				String tweetId = data[0];
				String text = data[1];
				String userId = data[2];
				String isRetweet = data[3];
				String userName = data[4];
				Put p = new Put(Bytes.toBytes(tweetId));
				p.add(Bytes.toBytes("text"), Bytes.toBytes("text"), Bytes.toBytes(text));
				p.add(Bytes.toBytes("userId"), Bytes.toBytes("userId"), Bytes.toBytes(userId));
				p.add(Bytes.toBytes("userName"), Bytes.toBytes("userName"), Bytes.toBytes(userName));
				p.add(Bytes.toBytes("isRetweet"), Bytes.toBytes("isRetweet"), Bytes.toBytes(isRetweet));
				//tweetTable.put(p);
				putsTweets.add(p);
				
				Put p2 = new Put(Bytes.toBytes(userId));
				p2.add(Bytes.toBytes("username"), Bytes.toBytes("username"), Bytes.toBytes(userName));
				//userTable.put(p2);
				putsUsers.add(p2);
			}	 
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (br != null) {
				try {
					br.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}
}
