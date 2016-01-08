package DataCrawler.Token;

import java.util.Date;

/**
 * Created by 叶 on 2015/5/19.
 *
 */
public class Token implements Comparable<Token>{

    public String token;
    int count;
    long timestamp;

    /*oauth2.0*/
    Token(String at)
    {
        token = at;
        count = 0;
        timestamp = (new Date()).getTime();
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void addCount(){
        this.count ++;
    }

    @Override
    public int compareTo(Token o) {
        if (count < o.getCount())
            return -1;
        else if (count > o.getCount())
            return 1;

        if (timestamp < o.getTimestamp())
            return -1;
        else if((timestamp > o.getTimestamp()))
            return 1;
        return 0;
    }
}
