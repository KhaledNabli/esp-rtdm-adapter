package com.sas.coeci.esp;

import java.util.Properties;
import java.util.concurrent.ExecutorService;

import com.sas.coeci.esp.rdm.RDMEngine;

public class DSAdapterContext {

	private RDMEngine rtdmEngine;
	private String rtdmEventName;
	private Properties configProperties;
	private ExecutorService executor;

	public RDMEngine getRtdmEngine() {
		return rtdmEngine;
	}

	public void setRtdmEngine(RDMEngine rtdmEngine) {
		this.rtdmEngine = rtdmEngine;
	}

	public String getRtdmEventName() {
		return rtdmEventName;
	}

	public void setRtdmEventName(String rtdmEventName) {
		this.rtdmEventName = rtdmEventName;
	}

	public Properties getConfigProperties() {
		return configProperties;
	}

	public void setConfigProperties(Properties configProperties) {
		this.configProperties = configProperties;
	}

	public ExecutorService getExecutor() {
		return executor;
	}
	
	public void setExecutor(ExecutorService executor) {
		this.executor = executor;
	}
	
}
