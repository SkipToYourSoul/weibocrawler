package DataCrawler;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import DataCrawler.Token.Token;
import DataCrawler.Token.TokenManage;
import Util.Tool;
import weibo4j.Weibo;
import weibo4j.WeiboException;
import weibo4j.http.Response;
import weibo4j.model.Status;
import weibo4j.model.StatusWapper;

/**
 * 
 * @modify xiafan
 * @date 2015.12.24
 *
 */
public class Controller {
	static {
		PropertyConfigurator
				.configure(new File(System.getProperty("basedir", "./"), "conf/log4j.properties").getAbsolutePath());
	}

	private static final Logger logger = Logger.getLogger(Controller.class);
	public final static int sleepTime = 4000;
	private static final int RETRY_NUM = 10;
	private int curSleepTime = sleepTime;
	/**
	 * 用于Token管理的变量
	 */
	protected static Weibo weibo = new Weibo();
	protected static TokenManage tm = new TokenManage();
	protected static Token tokenPack = tm.GetToken();
	String taskName;

	public Controller(String taskName) {
		this.taskName = taskName;
	}

	/**
	 * 用于操作配置文件
	 */
	protected static Configuration configuration = new Configuration();

	/**
	 * 时间格式管理变量
	 */
	protected SimpleDateFormat inputFormat = new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss");
	protected SimpleDateFormat inputFormatYMD = new SimpleDateFormat("yyyy-MM-dd");
	protected SimpleDateFormat inputFormatYMDH = new SimpleDateFormat("yyyy-MM-dd-HH");

	static {
		configuration.load(System.getProperty("basedir", "./") + "/conf/RC.conf");
		tm.setMaxCount(configuration.getMaxTokenCount());
		weibo.setToken(tokenPack.token);
	}

	protected static void createNewFile(String path) {
		File file = new File(path);
		if (!file.exists()) {
			try {
				file.createNewFile();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	protected static void createFileDir(String path) {
		File fileDir = new File(path);
		if (!fileDir.exists()) {
			fileDir.mkdirs();
		}
	}

	public abstract class CrawlerWorker {
		public Response res = null;

		public abstract void cralwerImpl() throws WeiboException, weibo4j.model.WeiboException;

		public Response getRes() {
			return res;
		}

	}

	public List<Status> cralwStatus(CrawlerWorker worker) {
		List<Status> statusList = new ArrayList<Status>();

		StatusWapper wapper = null;
		if (cralw(worker)) {
			try {
				Response res = worker.getRes();
				// 解析
				if (!res.toString().equals("[]") && res != null) {
					wapper = Status.constructWapperStatus(res.toString());
					if (wapper == null) {
						return statusList;
					}
					statusList = wapper.getStatuses();
				}
				if (res.toString().startsWith("[")) {
					statusList = Status.constructStatuses(Tool.removeEol(res.toString()));
				}
			} catch (WeiboException e) {
				// 解析时报的异常
			} catch (weibo4j.model.WeiboException e) {
				// 解析时报的异常
			} catch (Exception e) {

			}
		}
		return statusList;
	}

	public boolean cralw(CrawlerWorker worker) {
		boolean ret = false;
		for (int i = 0; i < RETRY_NUM; i++) {
			try {
				worker.cralwerImpl();
				tokenPack.addCount();
				if (!tm.maxTokenCount(tokenPack)) {
					tokenPack = tm.GetNextToken();
					weibo.setToken(tokenPack.token);
				}
				curSleepTime = sleepTime;
				randomSleep(curSleepTime, taskName);
				ret = true;
				break;
			} catch (WeiboException e) {
				// 解析时报的异常
			} catch (weibo4j.model.WeiboException e) {
				logger.error(e.getMessage());
				// 获取时报的异常
				if (e.getStatusCode() == -1) {
					// 网络连接超时
					i = 0;
				} else if (e.getStatusCode() == 401) {
					tokenPack = tm.GetNextToken();
					weibo.setToken(tokenPack.token);
				} else if (e.getStatusCode() == 403) {
					if (e.getMessage().contains("invalid_access_token")) {
						tokenPack = tm.GetNextToken();
						weibo.setToken(tokenPack.token);
					}
					randomSleep(curSleepTime, taskName);
					curSleepTime += curSleepTime * 0.3;
				} else if (e.getStatusCode() == 400) {
					// This is the status code will be returned during rate
					// limiting.error:expired_token
					// error_code:21327/2/statuses/repost_timeline.json
					tokenPack = tm.GetNextToken();
					weibo.setToken(tokenPack.token);
				}
			} catch (Exception e) {
				logger.error(e.getMessage());
			}
		}
		return ret;
	}

	protected void randomSleep(int sleeping, String thread) {
		Random ra = new Random();
		int random = ra.nextInt(2000) - 1000;
		logger.info(thread + " sleep " + (sleeping + random) + "ms!");
		try {
			Thread.sleep(sleeping + random);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
}
