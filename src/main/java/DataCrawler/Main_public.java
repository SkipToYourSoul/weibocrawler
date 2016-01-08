package DataCrawler;

import java.io.FileInputStream;
import java.util.Properties;

import xiafan.perf.MetricBasedPerfProfile;

public class Main_public {

	public static void main(String[] args) throws Exception {
		if (args.length != 1) {
			System.out.println("usage: path to configuration file");
			System.exit(1);
		}

		Properties props = new Properties();
		props.load(new FileInputStream(args[0]));

		MetricBasedPerfProfile.reportForGanglia(props.getProperty("gangliaHost"),
				Short.parseShort(props.getProperty("gangliaPort")));

		GetPublicStatusThread publicThread = new GetPublicStatusThread();
		publicThread.init(props.getProperty("kafka"));
		new Thread(publicThread).start();

		GetUserStatusThread userThread = new GetUserStatusThread();
		userThread.init(props.getProperty("kafka"), props.getProperty("cassdb"));
		new Thread(userThread).start();
	}
}
