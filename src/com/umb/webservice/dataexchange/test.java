package com.umb.webservice.dataexchange;

import com.umb.webservice.dao.BusinessDao;
import com.umb.webservice.dao.impl.BusinessDaoImp;

public class test {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		String[][] proviceCode = {{"AH","34"},
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
		// TODO Auto-generated method stub
		String workNo = "111600000001";
        String proviceCodeStr = new DataExchange().getProviceCode(workNo);
        String proviceCodeJC = null;
        for (int i = 0; i < proviceCode.length; i++) {
			if(proviceCode[i][1].equals(proviceCodeStr))
			{
				proviceCodeJC = proviceCode[i][0];
			}
		}
        BusinessDao busDao = new BusinessDaoImp();
        busDao.executeQuery(workNo,proviceCodeJC);

	}
}
 	