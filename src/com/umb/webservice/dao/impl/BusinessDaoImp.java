package com.umb.webservice.dao.impl;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import oracle.jdbc.OracleTypes;
import oracle.jdbc.driver.OracleCallableStatement;

import com.umb.webservice.dao.BusinessDao;
import com.umb.webservice.dblink.DBLink;
import com.umb.webservice.tool.ReadTableInfo;
import com.umb.webservice.tool.ReadXml;

public class BusinessDaoImp implements BusinessDao {

	private final String connUrl ="/DbInfo/db-connection.xml";
	private final String tburl = "/DbInfo/table-config.xml";
	@Override
	public List<Map<String, Object>> executeQuery(String str, String proviceCode) {
		Connection conn = null;
		PreparedStatement ps = null;
		Map<String, String> sourceMap = new ReadXml().getSourceLink(connUrl);
		//System.out.println(sourceMap.get("port"));
		try {
			conn = DBLink.getConnection(sourceMap);
			List<Map<String, Object>> tableList = new ReadTableInfo().getTableDetatil(tburl);
			for (int i = 0; i < tableList.size(); i++) {
				List<Map<String, Object>> resultList = new ArrayList<Map<String, Object>>();
				Map<String, Object> tableMap = tableList.get(i);
				String tableName = String.valueOf(tableMap.get("tableName"));
				String columnCount = String
						.valueOf(tableMap.get("cloumCount"));
				String ruleId = String.valueOf(tableMap.get("roleId"));// 插入日志表ETL_TABLE_LOG使用
				List colMapList = (List) tableMap.get("column");
				List<String> colList = new ArrayList<String>();
				List<String> colTypeList = new ArrayList<String>();
				for (int m = 0; m < colMapList.size(); m++) {
					Map colMap = (Map) colMapList.get(m);
					String colName = String.valueOf(colMap.get("colName"));
					String colType = String.valueOf(colMap.get("colType"));
					colList.add(colName);
					colTypeList.add(colType);
				}
				StringBuffer sql = new StringBuffer();
				sql.append("select  ");
				for (int j = 0; j < Integer.parseInt(columnCount); j++) {
					if (j < Integer.parseInt(columnCount) - 1) {
						if (colTypeList.get(j).equals("timestamp")) {
							sql.append("to_char(" + colList.get(j)
									+ ",'yyyymmdd hh24:mi:ss') TIME_STAMP,");
						} else {
							sql.append(colList.get(j) + ",");
						}
					} else {
						if (colTypeList.get(j).equals("timestamp")) {
							sql.append("to_char(" + colList.get(j)
									+ ",'yyyymmdd hh24:mi:ss') TIME_STAMP");
						} else {
							sql.append(colList.get(j) + " ");
						}
					}
				}
				sql.append("  from  " + tableName + "_"+proviceCode);
				if (str != null && !"".equals(str) && isExist(colList)) {
					sql.append("  where WORK_NO=?"); // 参数
				}
				if (ruleId != null && !"".equals(ruleId)) {
					sql.append(" order by ruleid asc");
				}
				ps = conn.prepareStatement(sql.toString(),
						ResultSet.TYPE_SCROLL_INSENSITIVE,
						ResultSet.CONCUR_UPDATABLE);
				if (str != null && !"".equals(str) && isExist(colList)) {
					ps.setString(1, str);
				}
				ResultSet rs = ps.executeQuery();
				ResultSetMetaData rsmd = rs.getMetaData();
				int count = rsmd.getColumnCount();
				while (rs.next()) {
					Map<String, Object> map = new HashMap<String, Object>();
					for (int k = 0; k < count; k++) {
						map.put(colList.get(k),
								getCol(colList.get(k), colTypeList.get(k), rs));
					}
					resultList.add(map);
				}
				//对查出的一个表的数据，进行操作
				insertDate(resultList,tableName,columnCount,colList,colTypeList,ruleId,str);
			}
			checkProcedure(str);
		} catch (Exception e) {
			System.out.println("错误信息：" + e.getMessage());
			e.printStackTrace();
		} finally {
			DBLink.closePs(ps);
			DBLink.closeConnection(conn);
		}
		return null;
	}
 /**
 * 统计每一个ruleID的个数
 * @param list
 * @param ruleId
 * @return
 */
public Map<String,Integer> caculateRuleCount(List<Map<String, Object>> list,String ruleId){
	   Map<String,Integer> countMap = new HashMap<String,Integer>();
	   for (int i = 0; i < list.size(); i++) {
		   String ruleNo = String.valueOf(list.get(i).get(ruleId));
		   Integer rule = countMap.get(ruleNo);
		   if (rule == null) {
			   countMap.put(ruleNo, 1);
		   }
		   else{
				countMap.put(ruleNo, ++rule);
		   }
		   
	}
	   return countMap;
   }
	@Override
	public boolean insertDate(List<Map<String, Object>> list, String tableName,
			String columnCount, List<String> colList, List<String> colTypeList,String ruleId,String workNo)
			throws Exception {
		Map<String,Integer> countMap = caculateRuleCount(list, ruleId);
		Map<String, String> destnationMap = new ReadXml().getDestnationLink(connUrl);
		Connection conn = null;
		PreparedStatement ps = null;
		boolean flag = false;
		try{
			//插入表数据
			conn = DBLink.getConnection(destnationMap);
			//事务开始
			conn.setAutoCommit(false);
			deleteTableDate(conn, workNo, tableName,colList);
			StringBuffer sql = new StringBuffer();
			sql.append("insert into ");
			sql.append(tableName);
			sql.append("(");
			for (int j = 0; j < Integer.parseInt(columnCount); j++) {
				if (j<Integer.parseInt(columnCount)-1) {
					sql.append(colList.get(j)+",");
				}
				else{
					sql.append(colList.get(j));
				}
			}
			sql.append(") values(");
			for (int n = 0; n < Integer.parseInt(columnCount); n++) {
				if (n<Integer.parseInt(columnCount)-1) {
					addColumn(sql, colTypeList.get(n));
					sql.append(",");
				}
				else{
					addColumn(sql, colTypeList.get(n));
					sql.append(")");
				}
			}
			ps = conn.prepareStatement(sql.toString());
			for (int c = 0; c < list.size(); c++) {
				Map<String,Object> dataMap = list.get(c);
				for (int t = 0; t < Integer.parseInt(columnCount); t++){
					String strIn = ((dataMap.get(colList.get(t))) != null) ? String.valueOf(dataMap.get(colList.get(t))) : " ";
					ps.setString(t+1,strIn);
				}
//				System.out.println(c);
				int tav = ps.executeUpdate();
				if (tav > 0) {
					flag = true;
				}
				else{
					flag = false;
				}					
			}
			//插入日志数据  ruleId比为空才插入日志信息
			if (ruleId!=null && !"".equals(ruleId)) {
				Set<Map.Entry<String,Integer>> set = countMap.entrySet(); 
		        for(Iterator<Map.Entry<String,Integer>> it = set.iterator(); it.hasNext();){ 
		            Entry<String, Integer> entry = it.next(); 
	                String ruleNo = entry.getKey();
	                int count = entry.getValue();
		            deleteDataLog(conn,ruleNo, workNo);
		            inertDateLog(conn,count,ruleNo,workNo);
		        }
			}
			//事务完成
	        conn.commit();//提交JDBC事务		
			conn.setAutoCommit(true);// 恢复JDBC事务的默认提交方式,这是个好习惯;
		}catch(SQLException se){
			conn.rollback();
			System.out.println("错误信息："+se.getMessage());	
		}catch(Exception e)
		{
			System.out.println("错误信息："+e.getMessage());
		}
		finally{
			DBLink.closePs(ps);
			DBLink.closeConnection(conn);
		}
		return false;
	}
	
	/**
	 * 删除表数据
	 * @param conn
	 * @param workNo
	 * @param tableName
	 * @param colList
	 * @return
	 * @throws SQLException
	 */
	public boolean deleteTableDate(Connection conn,String workNo,String tableName,List<String> colList) throws SQLException{
    	
    	PreparedStatement ps = null;
    	boolean flag = false;
    	try{
    		String sql = "delete from "+tableName+" t";
    		if (isExist(colList)) {
				sql = sql+" where t.work_no = ?";
			}
    	    ps = conn.prepareStatement(sql);
    	    if (isExist(colList)) {
    	    	ps.setString(1, workNo);
			} 
    	    int i = ps.executeUpdate();
    	    if (i>0) {
				flag = true;
			}
    	}catch(SQLException e){
    		System.out.println("错误信息："+e.getMessage());
    		throw  e;
    	}
    	catch(Exception e){
    		System.out.println("错误信息："+e.getMessage());
    	}
    	finally {
			DBLink.closePs(ps);
		}
    	return flag;
    }
	/**
	 * 插入日志信息
	 * @param conn
	 * @param count
	 * @param ruleId
	 * @param workNo
	 * @throws Exception
	 */
	public void inertDateLog(Connection conn,int count,String ruleId,String workNo) throws Exception
    {
    	PreparedStatement ps = null;
    	Map<String, String> destnation = new ReadXml().getDestnationLink(connUrl);
    	try{
    		String sql = "insert into ETL_TABLE_LOG (WORK_NO,TABLE_NAME,SEND_COUNT,ACCESS_COUNT,WORK_RESULT,BEGIN_TIME,END_TIME,ROL_NAME,ACCESS_REMARK) values (?, ?, ?, ?, '成功', sysdate, sysdate, '无', '0')";
    	    ps = conn.prepareStatement(sql);
    	    ps.setString(1, workNo);
    	    ps.setString(2, ruleId);
    	    ps.setInt(3, count);
    	    ps.setInt(4, count);
    	    ps.executeUpdate();
    	}catch(SQLException e){
    		System.out.println("错误信息："+e.getMessage());
    		throw  e;
    	}catch(Exception e){
    		System.out.println("错误信息："+e.getMessage());
    	}
    	finally {
			DBLink.closePs(ps);
		}
    }
    /**
     * 删除日志信息
     * @param conn
     * @param ruleId
     * @param workNo
     * @return
     * @throws Exception
     */
    public boolean deleteDataLog(Connection conn,String ruleId,String workNo) throws Exception{
    	PreparedStatement ps = null;
    	boolean flag = false;
    	try{
    		String sql = "delete from etl_table_log t where t.work_no = ? and t.table_name = ?";
    	    ps = conn.prepareStatement(sql);
    	    ps.setString(1, workNo);
    	    ps.setString(2, ruleId);
    	    int i = ps.executeUpdate();
    	    if (i>0) {
				flag = true;
			}
    	}catch(SQLException e){
    		System.out.println("错误信息："+e.getMessage());
    		throw  e;
    	}catch(Exception e){
    		System.out.println("错误信息："+e.getMessage());
    	}
    	finally {
			DBLink.closePs(ps);
		}
    	return flag;
    }
    /**
     * 校验过程
     * @param
     */
    public void checkProcedure(String workNo){
    	String sysId = workNo.substring(0,2);
    	String param = "";
    	if (sysId.equals("01")) {
    		param = "KB";
		}else if (sysId.equals("02")) {
			param = "MX";
		}
    	
    	Connection conn = null;
    	Map<String, String> destnationMap = new ReadXml().getDestnationLink(connUrl);
    	try {
			conn = DBLink.getConnection(destnationMap);
			String sql = "{call PKG_FKSJJC_CK_DATA.P_FKSJJC_CK_DATA_ETL(?,?,?,?,?)}";
			CallableStatement call = conn.prepareCall(sql);
			OracleCallableStatement cs = (OracleCallableStatement)call;
			//
			  //设置输入参数
		    cs.setString(1, workNo);
		    cs.setString(2, param);
		    //设置输入参数
		    cs.registerOutParameter(3,OracleTypes.VARCHAR);
		    cs.registerOutParameter(4,OracleTypes.VARCHAR);
		    cs.registerOutParameter(5,OracleTypes.VARCHAR);
		    //执行
		    cs.execute();
		    //获取出参
		    String prame1 = cs.getString(3);
		    String prame2 = cs.getString(4);
		    String prame3 = cs.getString(4);
		    System.out.println("out_flag:"+prame1+"|"+"out_table_name:"+prame2+"|"+"out_fail_reason:"+prame3+",请详看输出日志信息表。");
		} catch (Exception e) {
			System.out.println("错误信息："+e.getMessage());
		}finally{
			DBLink.closeConnection(conn);
		}
    }
	/**
	 * 根据列的类型不同获取rs中的值
	 * 
	 * @param colName
	 * @param colType
	 * @param rs
	 * @return
	 * @throws SQLException
	 */
	public Object getCol(String colName, String colType, ResultSet rs)
			throws SQLException {
		Object obj = null;
		if (colType.equals("varchar")) {
			obj = rs.getString(colName);
		} else if (colType.equals("date")) {
			obj = rs.getDate(colName);
		} else if (colType.equals("number")) {
			obj = rs.getInt(colName);
		} else if (colType.equals("timestamp")) {
			obj = rs.getString(colName);
		} else {
			obj = rs.getString(colName);
		}
		return obj;
	}

	/**
	 * 根据列的类型拼装SQL
	 * @param sql
	 * @param colType
	 * @return
	 */
	public StringBuffer addColumn(StringBuffer sql, String colType) {
		if (colType.equals("varchar")) {
			sql.append("?");
		} else if (colType.equals("date")) {
			sql.append("to_date(?,'yyyy-mm-dd')");
		} else if (colType.equals("number")) {
			sql.append("to_number(?)");
		} else if (colType.equals("timestamp")) {
			sql.append("to_timestamp(?,'yyyy-mm-dd hh24:mi:ss')");
		} else {
			sql.append("?");
		}
		return sql;
	}

	/**
	 * 判断列中是否有workNo
	 * @param colList
	 * @return
	 */
	public boolean isExist(List<String> colList) {
		boolean flag = false;
		for (int j = 0; j < colList.size(); j++) {
			String colName = colList.get(j);
			if (colName.equals("WORK_NO")) {
				flag = true;
			}
		}
		return flag;
	}
}
