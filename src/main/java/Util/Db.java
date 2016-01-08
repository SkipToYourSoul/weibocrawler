package Util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

/**
 * Created by 叶 on 2015/5/13.
 *
 */
public class Db {
    private static final String DRIVERCLASS = "com.mysql.jdbc.Driver";
    private String url="jdbc:mysql://localhost:3306/";
    private String username="root";
    private String password="root";
    private String database = "";

    public void setDatabase(String database) {
        this.database = database;
    }

    public String getDatabase() {
        return database;
    }

    public Db(String database, String uName, String wPassword){
        url = url + database+"?useUnicode=true&characterEncoding=utf8";
        username = uName;
        password = wPassword;
    }

    public Db(String database, String ip, String username, String password){
        url = "jdbc:mysql://" + ip + ":3306/" + database+"?useUnicode=true&characterEncoding=utf8";
        this.username = username;
        this.password = password;
    }

    public Connection createConnection(){
        Connection conn=null;
        try{
            Class.forName(DRIVERCLASS);
            conn= DriverManager.getConnection(url, username, password);
        }catch (Exception e){
            e.printStackTrace();
        }
        return conn;
    }

    public PreparedStatement prepare(Connection conn, String sql){
        PreparedStatement ps=null;
        try{
            ps=conn.prepareStatement(sql);
        }catch (Exception e){
            e.printStackTrace();
        }
        return ps;
    }

    public void close(Connection conn){
        if(conn == null){
            return;
        }
        try{
            conn.close();
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    public void close(PreparedStatement ps){
        try{
            ps.close();
        }catch(Exception e){
            e.printStackTrace();
        }
    }

    public void close(ResultSet rs) {
        try {
            rs.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
