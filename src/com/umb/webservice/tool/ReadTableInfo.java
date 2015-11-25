package com.umb.webservice.tool;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


import org.jdom.Document;
import org.jdom.Element;
import org.jdom.input.SAXBuilder;



public class ReadTableInfo {
	public  List<Map<String, Object>> getTableDetatil(String url) {
		List<Map<String, Object>> tableList = new ArrayList<Map<String, Object>>();
		Map<String, Object> tableMap = null;
		Element element = null;
		InputStream is = this.getClass().getResourceAsStream(url);
		SAXBuilder sb = new SAXBuilder();
		try {
			Document dt = sb.build(is);
			element = dt.getRootElement();
			List<Element> tableEleList = element.getChildren("table");
			Element childrenRoot = null;
			for (int i = 0; i < tableEleList.size(); i++) {
				tableMap = new HashMap<String, Object>();
				childrenRoot = tableEleList.get(i);
				String tableName = childrenRoot.getAttributeValue("name");
				String cloumCount = childrenRoot.getAttributeValue("cloumCount");
				String roleId = childrenRoot.getAttributeValue("roleId");
				tableMap.put("tableName", tableName);
				tableMap.put("cloumCount", cloumCount);
				tableMap.put("roleId", roleId);
				List<Element> colEleList = childrenRoot.getChildren();
				List<Map<String,String>> columnList = new ArrayList<Map<String,String>>();
				for (int j = 0; j < colEleList.size(); j++) {
					Map<String,String> colMap = new HashMap<String,String>();
					Element ele = colEleList.get(j);
					String content = String.valueOf(ele.getContent().get(0));
					String type = ele.getAttributeValue("type");
					int index = content.indexOf("]");
					String colName = content.substring(6,index).trim();
					colMap.put("colName", colName);
					colMap.put("colType", type);
					columnList.add(colMap);
				}
				tableMap.put("column", columnList);
				tableList.add(tableMap);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return tableList;
	}
}
