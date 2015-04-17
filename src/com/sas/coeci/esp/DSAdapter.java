package com.sas.coeci.esp;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.SocketException;
import java.net.UnknownHostException;
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
	
	private static Properties readConfiguration(String filename) throws IOException {
		
		Properties configProps = new Properties();
		InputStream inputStream = new FileInputStream(new File(filename));
		configProps.load(inputStream);
		
		return configProps;
		
	}

	public static void main(String[] args) {
		Logger.info("Starting Adapter");
		Properties configProps;
		try {
			Configurator.fromFile(new File("esp-rtdm-adapter.properties")).activate();
			configProps = DSAdapter.readConfiguration("esp-rtdm-adapter.properties");
		} catch (IOException e) {
			e.printStackTrace();
			return;
		}
		
		
		
		
		String engineUrl = configProps.getProperty("esp.url");
		String rtdmHost = configProps.getProperty("rtdm.host");
		int rtdmPort = Integer.parseInt(configProps.getProperty("rtdm.port"));
		String rtdmEventName = configProps.getProperty("ESP_Twitter_Event");
		Level logLevel = Level.parse(configProps.getProperty("tinylog.level").toUpperCase());
		
		
		DSAdapterContext ctx = new DSAdapterContext();
		ctx.setRtdmEngine(new RDMEngine(rtdmHost, rtdmPort, configProps));
		ctx.setConfigProperties(new Properties());
		ctx.setRtdmEventName(rtdmEventName);
		
		dfESPclientHandler handler = new dfESPclientHandler();
		handler.init(logLevel);

		
		
		String schemaUrl = engineUrl.substring(0, engineUrl.indexOf('?')) + "?get=schema";
		Logger.info("Check ESP Url: {}", schemaUrl);
		ArrayList<String> schemaVector = null;
		
		try {
			schemaVector = handler.queryMeta(schemaUrl);
		} catch (UnknownHostException e) {
			Logger.error(e);
			return;
		}

		if(schemaVector == null || schemaVector.size() != 1)  {
			Logger.error("ESP Url is invalid. Please check the syntax and that ESP is running properly.");
			return;
		}
		
		
		Logger.info("Start subscribing to ESP Window");
		dfESPclient client = handler.subscriberStart(engineUrl, new DSAdapterCallbacks(), ctx);

		/* Now make the actual connection to the ESP application or server. */
		try {
			if (!handler.connect(client)) {
				Logger.error("Start subscribing failed. Can not connect to ESP window");
				System.exit(1);
			}
		} catch (SocketException | UnknownHostException e) {
			Logger.error(e);
		}
		
		
		
		
		/* Create a mostly non-busy wait loop. */
		while (nonBusyWait) {
			try {
				Logger.debug("Putting Thread to sleep for 1000ms");
				Thread.sleep(1000); // sleep for 1000 ms
			} catch (InterruptedException ie) {
				Logger.debug("Continue Thread to sleep for 1000ms");
			}
		}

		Logger.info("Stopping the ESP connection");
		handler.stop(client, true);
	}
}