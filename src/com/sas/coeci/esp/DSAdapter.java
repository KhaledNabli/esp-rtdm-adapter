package com.sas.coeci.esp;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Properties;
import java.util.logging.Level;

import org.pmw.tinylog.Configurator;
import org.pmw.tinylog.Logger;

import com.sas.coeci.esp.rdm.RDMEngine;
import com.sas.esp.api.pubsub.dfESPclient;
import com.sas.esp.api.pubsub.dfESPclientHandler;
/* These import files are needed for all subscribing code. */

public class DSAdapter {

	static private boolean nonBusyWait = true;
	static private String rtdmHost = "sasbap.demo.sas.com";
	static private int rtdmPort = 8680;
	static private String espWindowUrl = "dfESP://localhost:55555/project/contQuery/twitterEvent?snapshot=true";
	static private String rtdmEventName = "ESP_Twitter_Event";
	


	
	
	
	
	
	
	
	
	


	public static void main(String[] args) throws IOException {

		String engineUrl = espWindowUrl;
		
		Configurator.fromFile(new File("esp-rtdm-adapter.properties")).activate();
		
		Logger.info("Start Logging");
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		DSAdapterContext ctx = new DSAdapterContext();
		ctx.setRtdmEngine(new RDMEngine(rtdmHost, rtdmPort, new Properties()));
		ctx.setConfigProperties(new Properties());
		ctx.setRtdmEventName(rtdmEventName);
		
		dfESPclientHandler handler = new dfESPclientHandler();
		handler.init(Level.FINEST);

		
		
		String schemaUrl = engineUrl.substring(0, engineUrl.indexOf('?')) + "?get=schema";
		Logger.info("Check ESP Url: {}", schemaUrl);
		ArrayList<String> schemaVector = handler.queryMeta(schemaUrl);
		if(schemaVector.size()==1) {
			Logger.info("ESP Url is valid and has the schema {}", schemaVector.get(0));
		}
		else {
			Logger.error("ESP Url is invalid. Please check the syntax and that ESP is running properly.");
		}
		
		
		Logger.info("Start subscribing to ESP Window");
		dfESPclient client = handler.subscriberStart(engineUrl, new DSAdapterCallbacks(), ctx);

		/* Now make the actual connection to the ESP application or server. */
		if (!handler.connect(client)) {
			Logger.error("Start subscribing failed. Can not connect to ESP window");
			System.exit(1);
		}
		/* Create a mostly non-busy wait loop. */
		while (nonBusyWait) {
			try {
				//Logger.info("Putting Thread to sleep for 1000ms");
				Thread.sleep(1000); // sleep for 1000 ms
			} catch (InterruptedException ie) {
				//Logger.info("Continue Thread to sleep for 1000ms");
			}
		}

		Logger.info("Stopping the ESP connection");
		handler.stop(client, true);
	}
}