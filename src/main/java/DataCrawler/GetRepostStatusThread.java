package DataCrawler;

import java.util.List;

import com.codahale.metrics.Meter;
import com.codahale.metrics.Timer.Context;

import casdb.CrawlStateDao;
import common.TweetLifeCycleEvaluator;
import kafkastore.KafkaTopics;
import kafkastore.RepostCrawlState;
import kafkastore.TweetKafkaProducer;
import weibo4j.WeiboException;
import weibo4j.model.Paging;
import weibo4j.model.Status;
import xiafan.perf.MetricBasedPerfProfile;

public class GetRepostStatusThread extends Controller implements Runnable {
	TweetKafkaProducer producer;
	RepostCrawlState state;
	Paging paging = new Paging();
	int totalCount = 0;
	Status oStatus = null;
	RepostCrawler cralwer = new RepostCrawler();
	CrawlStateDao stateDao;

	public GetRepostStatusThread(TweetKafkaProducer producer, RepostCrawlState state, CrawlStateDao stateDao) {
		super("repost");
		this.producer = producer;
		this.state = state;
		this.stateDao = stateDao;
	}

	@Override
	public void run() {
		getRepostStatus();
	}

	private class RepostCrawler extends CrawlerWorker {
		@Override
		public void cralwerImpl() throws WeiboException, weibo4j.model.WeiboException {
			Context context = MetricBasedPerfProfile.timer("repost_crawler_time").time();
			try {
				res = weibo.getRepostTimeline1(state.mid, paging);
			} finally {
				context.stop();
			}
		}
	}

	public void getRepostStatus() {
		boolean shouldCrawl = false;
		if (state.lastCrawlTime == 0) {
			shouldCrawl = true;
		} else {
			RepostCrawlState tmpState = stateDao.getRepostStateByMid(state.mid);
			if (tmpState == null || tmpState.lastCrawlTime <= state.lastCrawlTime) {
				shouldCrawl = true;
			}
		}

		if (shouldCrawl) {
			Meter crawlRate = MetricBasedPerfProfile.meter("repost_crawl_rate");
			paging.setCount(200);
			paging.setSinceId(state.sinceID);
			totalCount = 0;
			long minTime = Long.MAX_VALUE;
			long maxTime = 0;

			for (int page = 1; page <= 10; page++) {
				paging.setPage(page);
				List<Status> statusList = cralwStatus(cralwer);
				crawlRate.mark(statusList.size());
				// 将结果写回文件
				if (statusList.size() > 0) {
					totalCount += statusList.size();
					// result = getStatus(statusList);
					for (Status status : statusList) {
						minTime = Math.min(status.getCreatedAt().getTime(), minTime);
						maxTime = Math.max(status.getCreatedAt().getTime(), maxTime);
						if (oStatus == null && status.getRetweetedStatus() != null
								&& status.getRetweetedStatus().getCreatedAt() != null) {
							oStatus = status.getRetweetedStatus();
						}

						producer.store(KafkaTopics.RETWEET_TOPIC, status);
						state.sinceID = Math.max(state.sinceID, status.getId());
					}

				} else {
					break;
				}
				System.out.println("--------------------------------->");
				System.out.println(
						"Thread_repost: get " + totalCount + " status of " + state.mid + " at " + tokenPack.getCount());
				System.out.println("--------------------------------->");
			}
			state.setLastCrawlNum((short) totalCount);
			state.setLastCrawlTime(System.currentTimeMillis());
			stateDao.putRepostState(state);
			float vec = ((float) totalCount) / ((maxTime - minTime) / (1000 * 3600) + 1);
			if (oStatus != null && TweetLifeCycleEvaluator.isHotTweet(oStatus, vec)) {
				producer.storeRepostCrawlState(
						new RepostCrawlState(oStatus.getMid(), 1, System.currentTimeMillis(), (short) totalCount));
			} else {
				// an indication for time series produce that we can approximate
				// the
				// timeseries of all nodes in the retweet tree
				producer.store(KafkaTopics.RETWEET_TOPIC, oStatus);
			}
			System.out.println("Thread_repost: " + state.mid + " complete!");
		}
	}

}
