package DataCrawler;

import java.util.List;
import java.util.concurrent.Semaphore;

import com.codahale.metrics.Meter;
import com.codahale.metrics.Timer;
import com.codahale.metrics.Timer.Context;

import common.TweetLifeCycleEvaluator;
import kafkastore.KafkaProducerFactory;
import kafkastore.KafkaTopics;
import kafkastore.RepostCrawlState;
import kafkastore.TweetKafkaProducer;
import weibo4j.WeiboException;
import weibo4j.model.Status;
import xiafan.perf.MetricBasedPerfProfile;
import xiafan.perf.ServerController;

/**
 * 
 * @modify xiafan
 * @date 2015.12.24
 *
 */
public class GetPublicStatusThread extends Controller implements Runnable, ServerController.IServerSubscriber {

	private TweetKafkaProducer producer;
	PublicCralwFunction cralwFunction = new PublicCralwFunction();
	Meter crawlRate;
	Timer avgCrawlDuration;
	Meter popNum;

	Semaphore susSem = new Semaphore(1);
	ServerController controler = new ServerController(this);

	public GetPublicStatusThread() {
		super("public status");
		crawlRate = MetricBasedPerfProfile.meter("public_crawler_rate");
		popNum = MetricBasedPerfProfile.meter("popular_microblog_from_public");
		avgCrawlDuration = MetricBasedPerfProfile.timer("public_crawler_time");
	}

	public void init(String servers) throws Exception {
		producer = new TweetKafkaProducer(KafkaProducerFactory.createProducer(servers));
		MetricBasedPerfProfile.registerServer(controler);
	}

	@Override
	public void run() {
		controler.running();
		while (true) {
			susSem.acquireUninterruptibly();
			try {
				// -------Crawl Status-------
				List<Status> statusList = cralwStatus(cralwFunction);

				crawlRate.mark(statusList.size());

				// 将结果写回文件
				for (Status status : statusList) {
					producer.store(KafkaTopics.TWEET_TOPIC, status);
					Status oStatus = status.getRetweetedStatus() == null ? status : status.getRetweetedStatus();
					if (TweetLifeCycleEvaluator.evaluate(oStatus, 0) > TweetLifeCycleEvaluator.THRESHOLD) {
						popNum.mark();
						producer.store(KafkaTopics.RETWEET_TOPIC, oStatus);
						producer.storeRepostCrawlState(new RepostCrawlState(oStatus.getMid(), 1, 0, (short) 0));
					}
				}
				System.out.println("--------------------------------->");
				System.out.println("Thread_public: get " + statusList.size() + " status at " + tokenPack.getCount());
				System.out.println("--------------------------------->");
				// --------------------------
			} finally {
				susSem.release();
			}
		}
	}

	private class PublicCralwFunction extends CrawlerWorker {
		@Override
		public void cralwerImpl() throws WeiboException, weibo4j.model.WeiboException {
			Context context = avgCrawlDuration.time();
			try {
				res = weibo.getPublicPlace(200);
			} finally {
				context.stop();
			}
		}
	}

	@Override
	public void finalize() {
		close();
	}

	public void close() {
		if (producer != null) {
			producer.close();
			producer = null;
		}
	}

	@Override
	public void onStop() {
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
