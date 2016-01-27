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
import java.util.concurrent.Executors;
import java.util.logging.Level;

import org.pmw.tinylog.Configurator;
import org.pmw.tinylog.Logger;

import com.sas.coeci.esp.rdm.RDMEngine;
import com.sas.esp.api.pubsub.dfESPclient;
import com.sas.esp.api.pubsub.dfESPclientHandler;

/* These import files are needed for all subscribing code. */

public class DSAdapter {

	private static Properties readConfigurationFromFile(String filename) throws IOException {

		Properties configProps = new Properties();
		InputStream inputStream = new FileInputStream(new File(filename));
		configProps.load(inputStream);
		configProps.setProperty("propertieFile", filename);

		return configProps;
	}

	private static String getConfigurationFileName(String[] args) {
		// if args.size() == 0 skip this
		String fileName = "rtdm-adapter.properties";
		if (args.length > 0) {
			Logger.debug("ARGS: {}", Arrays.toString(args));
			if (args[0].startsWith("-configFile=")) {
				// load configFile if exists and return properties
				fileName = args[0].replace("-configFile=", "").trim();
				Logger.info("Loading configuration from \"{}\"", fileName);
			} else {
				// error print usage command;
				Logger.warn("Command line argument {} is invalid. Argument will be ignored.", args[0]);
				Logger.info("Usage:\n\t./java -jar rtdm-adapter -configFile=my-rtdm-adapter.properties");
			}
		}
		Logger.debug("Configuration file suggested: {}", fileName);
		return fileName;
	}


	
	
	public static void main(String[] args) {

		String configFilename = getConfigurationFileName(args);
		File propertiesFile = new File(configFilename);
		Properties configProps = new Properties();

		// Setup Logging
		try {
			Configurator.fromFile(propertiesFile).activate();
		} catch (Exception e) {
			Logger.trace(e);
		}

		Logger.info("Starting RTDM Adapter for ESP 3.1");
		try {
			configProps = readConfigurationFromFile(getConfigurationFileName(args));
			Logger.debug("Configuration loaded successfully from file: {}", propertiesFile.getAbsoluteFile());
		} catch (Exception e) {
			Logger.error("Unable to load configuration from file: {}", propertiesFile.getAbsoluteFile());
			Logger.trace(e);
			System.exit(1);
		}

		
		String engineUrl = configProps.getProperty("esp.url");
		String rtdmHost = configProps.getProperty("rtdm.host");
		String rtdmEventName = configProps.getProperty("rtdm.event");
		String adapterThreadPool = configProps.getProperty("adapter.threadpool");
		boolean hasMissingProperties = false;

		// catch missing or invalid properties.
		if (engineUrl == null || engineUrl.isEmpty()) {
			Logger.error("Missing mandatory value for \"esp.url\" in configuration file.");
			hasMissingProperties = true;
		}

		if (rtdmHost == null || rtdmHost.isEmpty()) {
			Logger.error("Missing mandatory value for \"rtdm.host\" in configuration file.");
			hasMissingProperties = true;
		}

		if (rtdmEventName == null || rtdmEventName.isEmpty()) {
			Logger.error("Missing mandatory value for \"rtdm.event\" in configuration file.");
			hasMissingProperties = true;
		}

		int rtdmPort = 0;
		try {
			rtdmPort = Integer.parseInt(configProps.getProperty("rtdm.port"));
		} catch (Exception e) {
			Logger.error("Unable to get mandatory value for \"rtdm.port\" in configuration file because of {}.", e.getLocalizedMessage());
			Logger.trace(e);
			hasMissingProperties = true;
		}

		if (rtdmPort < 1) {
			Logger.error("Invalid value for \"rtdm.port\" in configuration file. rtdm.port = {}", rtdmPort);
			hasMissingProperties = true;
		}

		if (hasMissingProperties) {
			// Stop process due to missing parameter in properties file.
			Logger.error("Stopping RTDM Adapter due to errors in configuration file.");
			System.exit(1);
		}

		int threadPoolSize = 5;
		try {
			threadPoolSize = Integer.parseInt(adapterThreadPool);
		} catch (Exception e) {
			Logger.warn("Unable to get optional value for \"adapter.threadpool\" in configuration file because of {}. Setting thread pool size = {}", e.getLocalizedMessage(), threadPoolSize);
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
		ctx.setRtdmEngine(new RDMEngine(rtdmHost.trim(), rtdmPort, configProps));
		ctx.setConfigProperties(configProps);
		ctx.setRtdmEventName(rtdmEventName.trim());
		ctx.setExecutor(Executors.newFixedThreadPool(threadPoolSize));
		
		dfESPclientHandler handler = new dfESPclientHandler();
		handler.init(logLevel);

		String schemaUrl = engineUrl.substring(0, engineUrl.indexOf('?')) + "?get=schema";
		Logger.info("Checking ESP connection to {}", schemaUrl);
		ArrayList<String> schemaVector = null;

		try {
			schemaVector = handler.queryMeta(schemaUrl);
		} catch (Exception e) {
			Logger.error("Unable to get the window schema from ESP engine {}", engineUrl);
			Logger.trace(e);
		}

		if (schemaVector == null || schemaVector.size() != 1) {
			Logger.error("ESP Url is invalid. Please check the syntax and make sure that ESP is running.");
			System.exit(1);
		}

		
		// TODO check unsupported datatypes
		// TODO check data type matching
		String rtdmSchema = schemaVector.get(0).replace(":int64", ":Integer");
		rtdmSchema = rtdmSchema.replace(":int32", ":Integer");
		rtdmSchema = rtdmSchema.replace(":string", ":Character");
		rtdmSchema = rtdmSchema.replace(":double", ":Double");
		rtdmSchema = rtdmSchema.replace(":money", ":Double");
		rtdmSchema = rtdmSchema.replace(":date", ":Date");
		rtdmSchema = rtdmSchema.replace(":timestamp", ":Date");
		rtdmSchema = rtdmSchema.replace("*:", ":");
		rtdmSchema = rtdmSchema.replace(":", " :\t");
		rtdmSchema = rtdmSchema.replace(",", "\n");
		Logger.info("Adapter expects RTDM event definition to match: \n{}\n", rtdmSchema);
		
		
		

		Logger.info("Start subscribing to ESP window");
		dfESPclient client = handler.subscriberStart(engineUrl, new DSAdapterCallbacks(), ctx);

		/* Now make the actual connection to the ESP application or server. */
		// Start seperate thread for ESP Handling
		try {
			if (!handler.connect(client)) {
				Logger.error("Start subscribing failed. Can not connect to ESP window");
				System.exit(1);
			}
		} catch (SocketException | UnknownHostException e) {
			Logger.error(e.getLocalizedMessage());
			Logger.trace(e);
		}
	}
}