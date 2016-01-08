package DataCrawler;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Semaphore;

import com.codahale.metrics.Meter;
import com.codahale.metrics.Timer;
import com.codahale.metrics.Timer.Context;

import casdb.CassandraConn;
import casdb.CrawlStateDao;
import common.TweetLifeCycleEvaluator;
import kafkastore.KafkaProducerFactory;
import kafkastore.KafkaTopics;
import kafkastore.RepostCrawlState;
import kafkastore.TweetConsumer;
import kafkastore.TweetKafkaProducer;
import kafkastore.UserCrawlState;
import weibo4j.WeiboException;
import weibo4j.model.Paging;
import weibo4j.model.Status;
import xiafan.perf.MetricBasedPerfProfile;
import xiafan.perf.ServerController;

/**
 * 
 * @modify xiafan
 * @date 2015.12.24
 *
 */
public class GetUserStatusThread extends Controller implements Runnable, ServerController.IServerSubscriber {
	private TweetKafkaProducer producer;
	private TweetConsumer consumer;
	UserCrawlState curState;
	Paging paging = new Paging();
	UserStatusFunction func = new UserStatusFunction();
	CrawlStateDao dao;
	CassandraConn conn;

	// 性能统计代码
	Meter crawlRate;
	Timer avgCrawlDuration;
	Meter popNum;

	// 用于远程控制服务器的代码
	Semaphore susSem = new Semaphore(1);
	ServerController controller = new ServerController(this);

	public GetUserStatusThread() {
		super("user status");
		crawlRate = MetricBasedPerfProfile.meter("user_crawler_rate");
		avgCrawlDuration = MetricBasedPerfProfile.timer("user_crawler_time");
		popNum = MetricBasedPerfProfile.meter("popular_microblog_from_user");
	}

	public void init(String servers, String dbServer) throws Exception {
		consumer = new TweetConsumer();
		consumer.open(Arrays.asList(KafkaTopics.VIP_UID_TOPIC), KafkaTopics.EMONITOR_GROUP, servers);
		producer = new TweetKafkaProducer(KafkaProducerFactory.createProducer(servers));
		conn = new CassandraConn();
		conn.connect(dbServer);
		dao = new CrawlStateDao(conn);
		MetricBasedPerfProfile.registerServer(controller);
	}

	@Override
	public void run() {
		controller.running();
		while (true) {
			// 找到上一次爬取的用户位置，继续爬取
			// 一轮又一轮的扫描uid
			susSem.acquireUninterruptibly();
			try {
				for (UserCrawlState state : consumer.nextCrawlerStates()) {
					curState = state;
					getUserStatus();
				}
			} finally {
				susSem.release();
			}
		}
	}

	@Override
	public void finalize() {
		close();
	}

	private void close() {
		if (conn != null) {
			conn.close();
			consumer.close();
		}
	}

	private class UserStatusFunction extends CrawlerWorker {
		@Override
		public void cralwerImpl() throws WeiboException, weibo4j.model.WeiboException {
			Context context = avgCrawlDuration.time();
			try {
				res = weibo.getUserTimelineByUidTest(curState.uid, paging);
			} finally {
				context.stop();
			}
		}

	}

	public void getUserStatus() {
		UserCrawlState tmp = dao.getUserCrawlStateByUid(curState.uid);
		if (tmp == null || curState.lastCrawlTime == 0 || curState.lastCrawlTime >= tmp.lastCrawlTime) {
			// 设置sinceId
			paging.setCount(200);
			paging.setPage(1);
			paging.setSinceId(curState.sinceID);
			if (tmp != null || curState.lastCrawlTime == 0) {
				paging.setSinceId(tmp.sinceID);
			}

			// 爬取过程，断网不跳出
			List<Status> statusList = cralwStatus(func);
			crawlRate.mark(statusList.size());

			// 存储结果
			for (Status status : statusList) {
				curState.setSinceID(Math.max(curState.sinceID, status.getId()));
				producer.store(KafkaTopics.TWEET_TOPIC, status);
				Status oStatus = status.getRetweetedStatus() == null ? status : status.getRetweetedStatus();
				if (TweetLifeCycleEvaluator.isHotTweet(oStatus, 0)) {
					popNum.mark();
					producer.store(KafkaTopics.RETWEET_TOPIC, oStatus);
					producer.storeRepostCrawlState(new RepostCrawlState(oStatus.getMid(), 1, 0, (short) 0));
				}
			}
			curState.setLastCrawlNum((short) statusList.size());
			curState.setLastCrawlTime(System.currentTimeMillis());
			producer.storeUserCrawlState(curState);
			dao.putUserCrawlState(curState);
			System.out.println("--------------------------------->");
			System.out.println("Thread_user: " + "Crawl " + curState.toString() + " get " + statusList.size()
					+ " status at " + tokenPack.getCount());
			System.out.println("--------------------------------->");
		} else {
			System.out.println("stale crawl state" + curState);
		}
	}

	@Override
	public void onStop() {
		susSem.acquireUninterruptibly();
		close();
	}

	@Override
	public void onSuspend() {
		susSem.acquireUninterruptibly();
	}

	@Override
	public void onResume() {
		susSem.release();
	}
}
