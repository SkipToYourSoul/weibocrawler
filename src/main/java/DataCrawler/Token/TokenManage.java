package DataCrawler.Token;

import java.io.File;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import Util.Tool;

/**
 * Created by 叶 on 2015/5/19.
 *
 */
public class TokenManage {

	static TokensPool tokenspool = null;
	int maxCount = 1000;
	static String LogDir = System.getProperty("basedir", ".") + File.separator + "rData" + File.separator + "Logs"
			+ File.separator;
	static SimpleDateFormat inputFormatYMD = new SimpleDateFormat("yyyy-MM-dd");

	public TokenManage() {
		tokenspool = new TokensPool();
	}

	public void setMaxCount(int maxCount) {
		this.maxCount = maxCount;
	}

	public Token GetToken() {
		Token tokenPack = null;
		tokenPack = tokenspool.getTokenList().get(0);
		return tokenPack;
	}

	public synchronized Token GetNextToken() {
		Token tokenPack = null;
		int nextPos = tokenspool.getPos() + 1;
		tokenspool.setPos(nextPos);
		if (tokenspool.IsOver()) {
			// TokenManage.refreshToken();
			tokenspool.setPos(0);
		}
		tokenPack = tokenspool.getTokenList().get(tokenspool.getPos());

		if (tokenPack == null) {
			System.out.println("Token error");
			return null;
		}

		tokenPack.setCount(0);
		System.out.println("Change Token : " + tokenPack.token);
		// Tool.write(LogDir + inputFormatYMD.format(new Date().getTime()) +
		// "token","change token: "+tokenPack.token,true,"utf-8");
		return tokenPack;
	}

	public boolean maxTokenCount(Token token) {
		if (token.getCount() > maxCount)
			return false;
		else
			return true;
	}

	public static boolean refreshToken() {
		GetAccessToken wc = new GetAccessToken();
		try {
			wc.run();
			System.out.println("refresh token finished");
			Tool.write(LogDir + inputFormatYMD.format(new Date().getTime()) + "token", "Refresh token!", true, "utf-8");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			// e.printStackTrace();
			return false;
		} catch (ParseException e) {
			return false;
			// TODO Auto-generated catch block
			// e.printStackTrace();
		}
		return true;
	}
}
