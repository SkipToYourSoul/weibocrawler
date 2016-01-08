package DataCrawler;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import casdb.CassandraConn;
import casdb.CrawlStateDao;
import kafkastore.KafkaProducerFactory;
import kafkastore.KafkaTopics;
import kafkastore.RepostCrawlState;
import kafkastore.TweetConsumer;
import kafkastore.TweetKafkaProducer;
import xiafan.perf.MetricBasedPerfProfile;

public class Main_repost {

	/**
	 * 用于爬取热门转发的线程池
	 */
	ThreadPoolExecutor executor = new ThreadPoolExecutor(5, 5, 200, TimeUnit.MILLISECONDS,
			new LinkedBlockingQueue<Runnable>());

	private TweetConsumer consumer;
	private TweetKafkaProducer producer;
	private CrawlStateDao stateDao;
	CassandraConn conn;

	public Main_repost(String servers, String dbServer) {
		consumer = new TweetConsumer();
		consumer.open(Arrays.asList(KafkaTopics.RTCRALW_STATE_TOPIC), KafkaTopics.RETWEET_CRAWLER_GROUP, servers);
		producer = new TweetKafkaProducer(KafkaProducerFactory.createProducer(servers));
		conn = new CassandraConn();
		conn.connect(dbServer);
		stateDao = new CrawlStateDao(conn);
	}

	private void start() {
		while (true) {
			for (RepostCrawlState state : consumer.nextRepostStates()) {
				GetRepostStatusThread repostThread = new GetRepostStatusThread(producer, state, stateDao);
				executor.execute(repostThread);

				try {
					while (executor.getQueue().size() > 0) {
						Thread.sleep(5000);
					}
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				System.out.println("线程池中线程数目：" + executor.getPoolSize() + "，队列中等待执行的任务数目：" + executor.getQueue().size()
						+ "，已执行完别的任务数目：" + executor.getCompletedTaskCount());

			}
		}
	}

	public static void main(String[] args) throws FileNotFoundException, IOException {
		if (args.length != 1) {
			System.out.println("usage: path to configuration file");
			System.exit(1);
		}

		Properties props = new Properties();
		props.load(new FileInputStream(args[0]));
		MetricBasedPerfProfile.reportForGanglia(props.getProperty("gangliaHost"),
				Short.parseShort(props.getProperty("gangliaPort")));

		Main_repost repost = new Main_repost(props.getProperty("kafka"), props.getProperty("cassdb"));
		repost.start();
	}
}
