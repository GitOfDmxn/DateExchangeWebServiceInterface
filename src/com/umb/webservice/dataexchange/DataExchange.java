package com.umb.webservice.dataexchange;

import umb.soa.services.mapservice.AbstractPrevPostDataProcessor;

import com.umb.webservice.dao.BusinessDao;
import com.umb.webservice.dao.impl.BusinessDaoImp;

public class DataExchange extends AbstractPrevPostDataProcessor{
    public static int cnt=0;
    private String[][] proviceCode = {{"AH","34"},
						    		  {"BJ","11"},
						    		  {"FJ","35"},
						    		  {"GS","62"},
						    		  {"HE","13"},
						    		  {"HA","41"},
						    		  {"LJ","23"},
						    		  {"HB","42"},
						    		  {"HN","43"},
						    		  {"JL","22"},
						    		  {"JB","16"},
						    		  {"JS","32"},
						    		  {"JX","36"},
						    		  {"LN","21"},
						    		  {"MD","15"},
						    		  {"NX","64"},
						    		  {"QH","63"},
						    		  {"SD","37"},
						    		  {"SX","14"},
						    		  {"SHX","61"},
						    		  {"SH","31"},
						    		  {"SC","51"},
						    		  {"TJ","12"},
						    		  {"XZ","54"},
						    		  {"XJ","65"},
						    		  {"ZJ","33"},
						    		  {"CQ","50"}};
	@Override
	public void process() throws Exception {
		System.out.println("START be called No." + cnt++);
		//获取输入参数
		String inputStr = this.getInputInfo();
		//获取workNo
		String workNo = inputStr;
		String proviceCodeStr = getProviceCode(workNo);
		String proviceJC = "";
        for (int i = 0; i < proviceCode.length; i++) {
			if(proviceCode[i][1].equals(proviceCodeStr))
			{
				proviceJC = proviceCode[i][0];
			}
		}
		BusinessDao busDao = new BusinessDaoImp();
		busDao.executeQuery(workNo,proviceJC);
	}
	public String getProviceCode(String workNo){
		return workNo.substring(2, 4);
	}
	
}
