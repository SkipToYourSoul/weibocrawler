package DataCrawler.Token;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class GetAccessToken {

    private List<String> urls;
    private Set<String> finishedUrls;
    private int count;
    private String fileDir;
    private int max;

    public GetAccessToken() {
        urls = new ArrayList<String>();
        finishedUrls = new HashSet<String>();
        count = 0;
        fileDir = ".\\";
        max = 100;
    }

    /*
     * 抓取一个网页并返回网页字符串
     */
    public String accessUrl(String url) {
        StringBuffer sb = new StringBuffer();
        try {
            URL u = new URL(url);
            HttpURLConnection huc = (HttpURLConnection) u.openConnection();
            huc.setConnectTimeout(3000);
            huc.setReadTimeout(3000);
            //String charset = getCharset(huc.getContentType());//获取字符集，但经常获取不到
            BufferedReader br = null;
            //if(!"".equals(charset))
            //	 br = new BufferedReader(new InputStreamReader(huc.getInputStream(),charset));
            //else
            br = new BufferedReader(new InputStreamReader(huc.getInputStream()));

            huc.connect();
            String tmp = null;
            while ((tmp = br.readLine()) != null) {
                sb.append(tmp);
            }
            huc.disconnect();
            br.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        String content = sb.toString();
        int index = 0;
        while ((index = content.indexOf("href=/")) != -1) {
            content = content.substring(index + 6);
            String tmpUrl = content.substring(0, content.indexOf("/"));
            if (tmpUrl.startsWith("http"))
                urls.add(tmpUrl);
        }
        count++;
        return sb.toString();
    }

    /*
     * 获取网页的字符集
     */
    public String getCharset(String s) {
        String contentType = s;
        String[] values = contentType.split(";");
        String charset = "";
        for (String value : values) {
            value = value.trim();
            if (value.toLowerCase().startsWith("charset=")) {
                charset = value.substring("charset=".length());
            }
        }
        return charset;
    }

    public void addUrl(String url) {
        urls.add(url);
    }

    public void setFileDir(String dir) {
        fileDir = dir;
    }

    public void setMax(int m) {
        max = m;
    }

    /*
     * 循环爬取直到计数器达到最大值
     */
    public void work() {
        while (count < max && !urls.isEmpty()) {
            String url = urls.get(0);
            if (finishedUrls.contains(url)) {
                urls.remove(0);
                continue;
            }
            save(accessUrl(url));
            urls.remove(0);
            finishedUrls.add(url);
        }
    }

    /*
     * 存储网页内容
     */
    public void save(String s) {
        try {
            File f = new File(fileDir);
            if (!f.exists()) {
                f.mkdirs();
            }

            String[] str = s.split("},");
            FileWriter fw = new FileWriter(fileDir + String.valueOf(count) + ".txt", false);
            for (int i = 0; i < str.length; i++) {
                fw.append(str[i]);
                fw.append("\n");
            }
            fw.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void getAccessToken() throws IOException, ParseException {
        FileInputStream fis;
        InputStreamReader isr;
        BufferedReader br;

        File file = new File(fileDir + String.valueOf(count) + ".txt");
        fis = new FileInputStream(file);
        isr = new InputStreamReader(fis, "GBK");
        br = new BufferedReader(isr);

        String line = "";
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
        Date end = new Date();
        long endT = end.getTime();
        //System.out.print(sdf.format(endT));
        File file1 = new File("." + File.separator + "Tokens" + File.separator + "tokens.txt");
        ArrayList<String> tempList = new ArrayList<String>();
        while ((line = br.readLine()) != null) {
            String create_time = line.substring(line.indexOf("create_time") + 14, line.indexOf("create_time") + 32);
            String accesss_token = line.substring(line.indexOf("access_token") + 15, line.indexOf("access_token") + 47);
            Date start_time = sdf.parse(create_time);
            long startT = start_time.getTime();
            //System.out.println(startT);
            long mint = (endT - startT) / (1000);
            int hor = (int) mint / 3600;
            int day = (int) hor / 24;
            //System.out.println(day);

            if (day < 30) {
                tempList.add(accesss_token);
            }
        }
        //write the token to tokens.txt
        FileWriter fileWriter = null;
        BufferedWriter bw = null;
        try {
            fileWriter = new FileWriter("." + File.separator + "Tokens" + File.separator + "tokens.txt", false);
            bw = new BufferedWriter(fileWriter);
            for (String temp : tempList) {
                bw.append(temp);
                bw.newLine();
            }
            bw.close();
            fileWriter.close();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    public void run() throws IOException, ParseException {
        System.out.println("Refresh Token!");
        setFileDir("." + File.separator + "Tokens" + File.separator);
        addUrl("");
        work();
        getAccessToken();
    }

    public static void main(String[] args) {
        GetAccessToken wc = new GetAccessToken();
        try {
            wc.run();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (ParseException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        System.out.println("refresh token finished");
    }
}
