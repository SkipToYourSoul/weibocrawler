package Util;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

/**
 * Created by 叶 on 2015/6/9.
 *  对文件转换成List进行操作
 */
public class StringReaderOne {
    ArrayList<String> ids = null;
    int pos = 0;

    public StringReaderOne(String path){
        ids = new ArrayList<String>();
        Input(path);
    }

    public int getIdsSize(){
        return ids.size();
    }

    public int getPos()
    {
        return pos;
    }

    public void setPos(int pos)
    {
        if (ids != null && pos <= ids.size())
        {
            this.pos = pos;
        }
    }

    public boolean IsOver()
    {
        return (pos >= ids.size());
    }

    public void nextPos(){ pos++; }

    public String GetStrictNewID()
    {
        if (pos < ids.size())
            return ids.get(pos);
        else
            return null;
    }

    /**
     * 将文件读入List
     * @param path
     */
    void Input(String path){
        String inline = "";
        BufferedReader reader = null;
        try {
            reader = new BufferedReader(new FileReader(path));
        }
        catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        try {
            int c = 0;
            while ((inline = reader.readLine()) != null) {
                if(inline.length()==0) {
                    continue;
                }
                if (inline.charAt(0) == '/')
                    continue;
                inline = inline.trim();
                if (inline.equals(""))
                    continue;
                if(!ids.contains(inline)) {
                    ids.add(inline);
                }
            }
            reader.close();
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void updateIds(String path, int pos){
        String inline = "";
        BufferedReader reader = null;
        try {
            reader = new BufferedReader(new FileReader(path));
        }
        catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        try {
            int c = 1;
            while ((inline = reader.readLine()) != null) {
                if(inline.length()==0) {
                    continue;
                }
                if (inline.charAt(0) == '/')
                    continue;
                inline = inline.trim();
                if (inline.equals(""))
                    continue;
                if(!ids.contains(inline) && c>=pos) {
                    ids.add(inline);
                }
                c++;
            }
            reader.close();
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }
}
