package com.sas.coeci.esp;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
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

	private static Properties readConfigurationFromFile(String filename) throws IOException {

		Properties configProps = new Properties();
		InputStream inputStream = new FileInputStream(new File(filename));
		configProps.load(inputStream);
		configProps.setProperty("propertieFile", filename);

		return configProps;
	}

	private static String getConfigurationFileName(String[] args) {
		// if args.size() == 0 skip this
		String fileName = "esp-rtdm-adapter.properties";
		if (args.length > 0) {
			Logger.debug("ARGS: {}", Arrays.toString(args));
			if (args[0].startsWith("-configFile=")) {
				// load configFile if exists and return properties
				fileName = args[0].replace("-configFile=", "").trim();
				Logger.info("Loading configuration from File: {}", fileName);
			} else {
				// error print usage command;
				Logger.warn("Command Line Argument {} is invalid. Argument will be ignored.", args[0]);
				Logger.info("Usage:\n\trtdm-adapter -configFile=\"c:\\temp\\esp-rtdm-adapter.properties\"");
			}
		}
		Logger.debug("Configuration file suggested: {}", fileName);
		return fileName;
	}

	public static void main(String[] args) {
		Logger.info("Starting RTDM Adapter");

		String configFilename = getConfigurationFileName(args);
		File propertiesFile = new File(configFilename);
		Properties configProps = new Properties();

		try {
			Configurator.fromFile(propertiesFile).activate();
		} catch (Exception e) {
			Logger.trace(e);
		}

		try {
			configProps = readConfigurationFromFile(configFilename);
			Logger.info("Configuration loaded successfully from file: {}", propertiesFile.getAbsoluteFile());
		} catch (Exception e) {
			Logger.error("Unable to load configuration from file: {}", propertiesFile.getAbsoluteFile());
			Logger.trace(e);
			System.exit(1);
		}

		String engineUrl = configProps.getProperty("esp.url");
		String rtdmHost = configProps.getProperty("rtdm.host");
		String rtdmEventName = configProps.getProperty("rtdm.event");

		// catch missing or invalid properties.
		if (engineUrl == null || engineUrl.isEmpty()) {
			Logger.error("Missing mandatory value for \"esp.url\" in configuration file.");
			System.exit(1);
		}

		if (rtdmHost == null || rtdmHost.isEmpty()) {
			Logger.error("Missing mandatory value for \"rtdm.host\" in configuration file.");
			System.exit(1);
		}

		if (rtdmEventName == null || rtdmEventName.isEmpty()) {
			Logger.error("Missing mandatory value for \"rtdm.event\" in configuration file.");
			System.exit(1);
		}

		int rtdmPort = 0;
		try {
			rtdmPort = Integer.parseInt(configProps.getProperty("rtdm.port"));
		} catch (Exception e) {
			Logger.error("Unable to get mandatory value for \"rtdm.port\" in configuration file because of {}.", e.getLocalizedMessage());
			Logger.trace(e);
			System.exit(1);
		}

		Level logLevel;
		try {
			logLevel = Level.parse(configProps.getProperty("esp.loglevel").toUpperCase());
		} catch (Exception e) {
			Logger.warn(
					"Setting Logging Level for ESP failed because of {}. Logging for ESP is switched off. Please check the value of esp.loglevel in the configuration file.",
					e.getLocalizedMessage());
			Logger.trace(e);
			logLevel = Level.OFF;
		}

		
		
		
		
		DSAdapterContext ctx = new DSAdapterContext();
		ctx.setRtdmEngine(new RDMEngine(rtdmHost, rtdmPort, configProps));
		ctx.setConfigProperties(configProps);
		ctx.setRtdmEventName(rtdmEventName);

		dfESPclientHandler handler = new dfESPclientHandler();
		handler.init(logLevel);

		String schemaUrl = engineUrl.substring(0, engineUrl.indexOf('?')) + "?get=schema";
		Logger.info("Check ESP Url: {}", schemaUrl);
		ArrayList<String> schemaVector = null;

		try {
			schemaVector = handler.queryMeta(schemaUrl);
		} catch (UnknownHostException e) {
			Logger.error(e.getLocalizedMessage());
			Logger.trace(e);
			System.exit(1);
		}

		if (schemaVector == null || schemaVector.size() != 1) {
			Logger.error("ESP Url is invalid. Please check the syntax and that ESP is running properly.");
			System.exit(1);
		}
		
		
		// TODO check data type matching
		String rtdmSchema = schemaVector.get(0).replace(":int64", ":Long");
		rtdmSchema = rtdmSchema.replace(":int32", ":Integer");
		rtdmSchema = rtdmSchema.replace(":string", ":String");
		rtdmSchema = rtdmSchema.replace(":double", ":Double");
		// TODO add data type for date
		rtdmSchema = rtdmSchema.replace("*:", ":");
		rtdmSchema = rtdmSchema.replace(":", " :\t");
		rtdmSchema = rtdmSchema.replace(",", "\n");
		Logger.info("Adapter exptects RTDM Event Definition to match: \n{}\n", rtdmSchema);

		Logger.info("Start subscribing to ESP Window");
		dfESPclient client = handler.subscriberStart(engineUrl, new DSAdapterCallbacks(), ctx);

		/* Now make the actual connection to the ESP application or server. */
		try {
			if (!handler.connect(client)) {
				Logger.error("Start subscribing failed. Can not connect to ESP window");
				System.exit(1);
			}
		} catch (SocketException | UnknownHostException e) {
			Logger.error(e.getLocalizedMessage());
			Logger.trace(e);
		}

		/* Create a mostly non-busy wait loop. */
		while (nonBusyWait) {
			try {
				Logger.trace("Putting Thread to sleep for 1000ms");
				Thread.sleep(1000); // sleep for 1000 ms
			} catch (InterruptedException ie) {
				Logger.trace("Continue Thread to sleep for 1000ms");
			}
		}

		Logger.info("Stopping the ESP connection");
		handler.stop(client, true);
	}
}