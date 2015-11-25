package com.umb.webservice.tool;

import java.io.File;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.jdom.Document;
import org.jdom.Element;
import org.jdom.input.SAXBuilder;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

public class ReadXml {

	/**
	 * 读取XML文件
	 * @param url xml文件路径ַ
	 * @return
	 */
	public  List<Map<String, String>> ReadXML(String url) {
		List<Map<String, String>> connectionInfo = new ArrayList<Map<String, String>>();
		Map<String, String> dbmap = null;
		Element element = null;
		this.getClass().getResource("/DbInfo/db-connection.xml").getPath();
		InputStream is = this.getClass().getResourceAsStream(url);
		SAXBuilder sb = new SAXBuilder();
		//System.out.println("url:"+url);
		try {
			Document dt =  sb.build(is);
			element = dt.getRootElement();
			List<Element> connList = element.getChildren("connector");
			Element childrenRoot = null;
            //property元素集合
              List<Element> propertyList = null;
			for (int i = 0; i < connList.size(); i++) {
				 childrenRoot = connList.get(i);
				 String type = childrenRoot.getAttributeValue("type");
				 dbmap = new HashMap<String, String>();
				 dbmap.put("type", type);
				 propertyList = childrenRoot.getChildren();
				 for (int j = 0; j < propertyList.size(); j++) {
					Element ele = propertyList.get(j);
					String name = ele.getName();
					String content = String.valueOf(ele.getContent().get(0));
					int index = content.indexOf("]");
					String hostname = content.substring(6,index).trim();
					dbmap.put(name, hostname);
				}
				 connectionInfo.add(dbmap);
				}
		} catch (Exception e) {
			System.out.println("错误信息："+e.getMessage());
			e.printStackTrace();
		}
		return connectionInfo;
	}
	/**
	 * 获取源数据库信息
	 * @param url xml文件路径
	 * @return
	 */
	public  Map<String, String> getSourceLink(String url){
		Map<String,String> sourceMap = new HashMap<String,String>();
		List<Map<String, String>> connectionInfo = ReadXML(url);
		for(int i=0;i<connectionInfo.size();i++){
			Map<String, String> map = connectionInfo.get(i);
			String type = map.get("type");
			if (type.equals("source")) {
				sourceMap = map;
			}
		}
		return sourceMap;
	}
	/**
	 * 获取目标数据库信息
	 * @param url xml文件路径ַ
	 * @return
	 */
	public  Map<String, String> getDestnationLink(String url){
		Map<String,String> destMap = new HashMap<String,String>();
		List<Map<String, String>> connectionInfo = ReadXML(url);
		for(int i=0;i<connectionInfo.size();i++){
			Map<String, String> map = connectionInfo.get(i);
			String type = map.get("type");
			if (type.equals("destination")) {
				destMap = map;
			}
		}
		return destMap;
	}
}
