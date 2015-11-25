package com.umb.webservice.dao;

import java.util.List;
import java.util.Map;

public interface BusinessDao {
	/**
	 * 查询表数据
	 * @param str
	 * @param proviceCode
	 * @return
	 */
	public List<Map<String,Object>> executeQuery(String str,String proviceCode);
	/**
	 * 插入表数据
	 * @param list
	 * @param tableName
	 * @param columnCount
	 * @param colList
	 * @param colTypeList
	 * @param ruleId
	 * @param workNo
	 * @return
	 * @throws Exception
	 */
	public boolean insertDate(List<Map<String, Object>> list,String tableName,String columnCount,List<String> colList,List<String> colTypeList,String ruleId,String workNo) throws Exception;
}
