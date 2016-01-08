package DataCrawler;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class Configuration {
    Properties props = new Properties();

	public void load(String path) {
        try {
            props.load(new FileInputStream(path));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public String getUidFile(){
        return props.getProperty("uidFile");
    }

    public String getUidPosFile(){
        return props.getProperty("uidPosFile");
    }

    public String getUserMidDir(){
        return props.getProperty("userMidDir");
    }

    public String getLogDir(){
        return props.getProperty("logDir");
    }

    public String getStatusDir(){
        return props.getProperty("statusDir");
    }

    public String getMidFile(){
        return props.getProperty("midFile");
    }

    public String getMidPosFile(){
        return props.getProperty("midPosFile");
    }

    public int getMaxTokenCount(){
        return Integer.valueOf(props.getProperty("maxTokenCount", "200"));
    }

    public int getMinHotRepostConditon(){
        return Integer.valueOf(props.getProperty("minHotRepostConditon", "500"));
    }
}
