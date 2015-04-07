package com.sas.coeci.esp.test;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;

import com.sas.coeci.esp.rdm.RDMEngine;
import com.sas.coeci.esp.rdm.RDMParameter;
import com.sas.coeci.esp.rdm.RDMParameter.Datatype;

public class TestClass {

	public static void main(String[] args) {
		
		
		RDMEngine myEngine = new RDMEngine("sasbap.demo.sas.com", 8680);
		List<RDMParameter> rdmRequestParameter = new ArrayList<RDMParameter>();
		RDMParameter twName = new RDMParameter("tw_name", Datatype.String, "KhaledN4BL1");
		RDMParameter twLstTwt = new RDMParameter("last_tweet_message", Datatype.String, "");
		RDMParameter twCounts = new RDMParameter("tw_counts", Datatype.Double, "30");
		rdmRequestParameter.add(twName);
		rdmRequestParameter.add(twCounts);
		rdmRequestParameter.add(twLstTwt);
		
		try {
			System.out.println(myEngine.invokeRdm("ESP_Twitter_Event", rdmRequestParameter).getString("message"));
			
		} catch (ClassNotFoundException | SecurityException | IllegalArgumentException | NoSuchMethodException | InstantiationException
				| IllegalAccessException | InvocationTargetException e) {
			e.printStackTrace();
		}

	}

}
