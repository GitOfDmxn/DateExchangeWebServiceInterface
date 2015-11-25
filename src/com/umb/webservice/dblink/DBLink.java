package com.umb.webservice.dblink;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Map;

public class DBLink {

	/**
	 * @author dmxn
	 * @2015-11-12
	 * @comment
	 * @param driver
	 * @param url
	 * @param user
	 * @param password
	 * @return
	 * @throws Exception
	 */
	public static Connection getConnection(String driver, String url,
			String user, String password) throws Exception {
		Class.forName(driver);
		return DriverManager.getConnection(url, user, password);
	}

	public static Connection getConnection(Map<String,String> dblink) throws Exception {
		String driver = "oracle.jdbc.driver.OracleDriver";
		String ip = dblink.get("url");
		String user = dblink.get("user");
		String pwd = dblink.get("pwd");
		String port = dblink.get("port");
		String sid = dblink.get("sid");
		String url = "jdbc:oracle:thin:@"+ip+":"+port+":"+sid;
//		return null;
		return getConnection(driver, url, user, pwd);
	}

	/**
	 * 关闭连接
	 * 
	 * @param conn
	 */
	public static void closeConnection(Connection conn) {
		try {
			if (null != conn) {
				conn.close();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * 关闭Statement
	 * 
	 * @param conn
	 */
	public static void closePs(PreparedStatement ps) {
		try {
			if (null != ps) {
				ps.close();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * 关闭ResultSet
	 * 
	 * @param conn
	 */
	public static void closeRs(ResultSet rs) {
		try {
			if (null != rs) {
				rs.close();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
