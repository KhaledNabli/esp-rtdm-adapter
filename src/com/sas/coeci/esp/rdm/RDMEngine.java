package com.sas.coeci.esp.rdm;

import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.Properties;

import com.sas.tap.client.SASDSRequest;
import com.sas.tap.client.SASDSRequestFactory;
import com.sas.tap.client.SASDSResponse;

public class RDMEngine {

	private String host;
	private int port;

	public RDMEngine(String _host, int _port) {
		host = _host;
		port = _port;
	}

	public SASDSResponse invokeRdm(String eventName, List<RDMParameter> parameterList) throws ClassNotFoundException, SecurityException, IllegalArgumentException,
			NoSuchMethodException, InstantiationException, IllegalAccessException, InvocationTargetException {

		SASDSRequestFactory factory = SASDSRequestFactory.getInstance(getRdmUrl(), new Properties());
		SASDSRequest request = factory.create(eventName, getCorrelationId(), getTimezone());

		for (RDMParameter parameter : parameterList) {
			switch (parameter.getType()) {
			case String:
				request.setString(parameter.getName(), parameter.getValue());
				break;
			case Integer:
			case Long:
				request.setLong(parameter.getName(), Long.parseLong(parameter.getValue()));
				break;
			case Double:
				request.setDouble(parameter.getName(), Double.parseDouble(parameter.getValue()));
				break;
			case Boolean:
				request.setBoolean(parameter.getName(), Boolean.parseBoolean(parameter.getValue()));
				break;
			default:
				
				break;
			}
		}
		
		SASDSResponse response = request.execute();


		return response;
	}

	private String getRdmUrl() {
		return "http://" + host + ":" + port + "/RTDM/Custom";
	}

	private String getCorrelationId() {
		return Long.toHexString(System.currentTimeMillis());
	}

	private String getTimezone() {
		return "GMT";
	}
}
