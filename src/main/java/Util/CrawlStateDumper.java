package Util;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import casdb.CassandraConn;
import casdb.CrawlStateDao;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import kafkastore.KafkaProducerFactory;
import kafkastore.KafkaTopics;
import kafkastore.RepostCrawlState;
import kafkastore.TweetConsumer;
import kafkastore.TweetKafkaProducer;
import kafkastore.UserCrawlState;

/**
 * 将kafka中的抓取状态存储到数据库中，以便在整个系统更新或者重启之后，初始化系统的爬取状态
 * 
 * @author xiafan
 *
 */
public class CrawlStateDumper {
	private final Logger logger = Logger.getLogger(CrawlStateDumper.class);
	String kafkaServers;
	private CassandraConn conn;
	private CrawlStateDao stateDao;

	public CrawlStateDumper(String kafkaServers, String dbServers) {
		conn = new CassandraConn();
		conn.connect(dbServers);
		stateDao = new CrawlStateDao(conn);
		this.kafkaServers = kafkaServers;
	}

	public void dump() {
		TweetConsumer consumer = new TweetConsumer();
		consumer.open(Arrays.asList(KafkaTopics.RTCRALW_STATE_TOPIC, KafkaTopics.VIP_UID_TOPIC),
				KafkaTopics.STATE_STORE_GROUP, kafkaServers);
		try {
			while (true) {
				for (Entry<String, List<Object>> entry : consumer.nextStates().entrySet()) {
					if (entry.getKey().equals(KafkaTopics.VIP_UID_TOPIC)) {
						for (Object data : entry.getValue())
							stateDao.putUserCrawlState((UserCrawlState) data);
					} else if (entry.getKey().equals(KafkaTopics.RTCRALW_STATE_TOPIC)) {
						for (Object data : entry.getValue())
							stateDao.putUserCrawlState((UserCrawlState) data);
					} else {
						logger.warn("unrecognized topics" + entry.getKey());
					}
				}
			}
		} finally {
			consumer.close();
		}
	}

	public void loadRTState() {
		TweetKafkaProducer producer = new TweetKafkaProducer(KafkaProducerFactory.createProducer(kafkaServers));
		Iterator<RepostCrawlState> iter = stateDao.getAllRpCrawlState();
		while (iter.hasNext()) {
			RepostCrawlState state = iter.next();
			producer.storeRepostCrawlState(state);
		}
		producer.close();
	}

	public void loadUserState() {
		TweetKafkaProducer producer = new TweetKafkaProducer(KafkaProducerFactory.createProducer(kafkaServers));
		Iterator<UserCrawlState> iter = stateDao.getAllUserCrawlState();
		while (iter.hasNext()) {
			UserCrawlState state = iter.next();
			producer.storeUserCrawlState(state);
		}
		producer.close();
	}

	public void close() {
		stateDao.close();
		conn.close();
	}

	/**
	 * 导出/或者加载爬取状态的类
	 * 
	 * @param args
	 */
	public static void main(String[] args) {
		PropertyConfigurator.configure("conf/log4j.properties");
		//args = new String[] { "-c", "127.0.0.1", "-k", "localhost:9092" };
		OptionParser parser = new OptionParser();
		parser.accepts("c", "cassandra server address").withRequiredArg().ofType(String.class);
		parser.accepts("k", "kafka server address").withRequiredArg().ofType(String.class);
		parser.accepts("o", "operations: dump, loadrt, loaduser").withRequiredArg().ofType(String.class);
		OptionSet set = parser.parse(args);
		CrawlStateDumper dumper = new CrawlStateDumper(set.valueOf("k").toString(), set.valueOf("c").toString());
		if (set.valueOf("o").equals("dump")) {
			dumper.dump();
		} else if (set.valueOf("o").equals("loadrt")) {
			dumper.loadRTState();
		} else if (set.valueOf("o").equals("loaduser")) {
			dumper.loadUserState();
		}
	}
}
