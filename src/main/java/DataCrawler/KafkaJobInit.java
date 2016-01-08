package DataCrawler;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import kafkastore.KafkaProducerFactory;
import kafkastore.TweetKafkaProducer;
import kafkastore.UserCrawlState;

/**
 * 初始化vip用户的监控队列
 * 
 * @author xiafan
 *
 */
public class KafkaJobInit {

	public static void main(String[] args) throws IOException {
		// args = new String[] { "localhost:9092", "rData/Files/uid" };
		if (args.length != 2) {
			System.out.println("args: kafka server address, uid file");
			System.exit(1);
		}
		BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(args[1])));
		TweetKafkaProducer producer = new TweetKafkaProducer(KafkaProducerFactory.createProducer(args[0]));
		String line = null;
		int count = 0;
		while (null != (line = reader.readLine())) {
			for (String uid : parseUids(line)) {
				producer.storeUserCrawlState(new UserCrawlState(uid, 1, 0, (short) 0));
				if (++count % 100 == 0) {
					System.out.println(count);
				}
			}
		}
		reader.close();
		producer.close();
	}

	static Pattern p1 = Pattern.compile("id=([0-9]+)");
	static Pattern p2 = Pattern.compile("ouid:([0-9]+)");
	static Pattern p3 = Pattern.compile("^([0-9]+)$");

	private static List<String> parseUids(String line) {
		List<String> ret = new ArrayList<String>();
		Matcher matcher = null;
		if (false) {
			matcher = p1.matcher(line);
			while (matcher.find()) {
				String uid = matcher.group(1);
				ret.add(uid);
			}
		}

		matcher = p2.matcher(line);
		while (matcher.find()) {
			String uid = matcher.group(1);
			ret.add(uid);
		}
		matcher = p3.matcher(line);
		while (matcher.find()) {
			String uid = matcher.group(1);
			ret.add(uid);
		}

		return ret;
	}
}
