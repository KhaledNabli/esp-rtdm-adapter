package com.sas.coeci.esp;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.pmw.tinylog.Logger;

import com.sas.coeci.esp.rdm.RDMEngine;
import com.sas.coeci.esp.rdm.RDMParameter;
import com.sas.coeci.esp.rdm.RDMParameter.Datatype;
import com.sas.esp.api.dfESPException;
import com.sas.esp.api.pubsub.clientCallbacks;
import com.sas.esp.api.pubsub.clientFailureCodes;
import com.sas.esp.api.pubsub.clientFailures;
import com.sas.esp.api.pubsub.clientGDStatus;
import com.sas.esp.api.server.datavar;
import com.sas.esp.api.server.ReferenceIMPL.dfESPevent;
import com.sas.esp.api.server.ReferenceIMPL.dfESPeventblock;
import com.sas.esp.api.server.ReferenceIMPL.dfESPschema;

public class DSAdapterCallbacks implements clientCallbacks {
	
	@Override
	public void dfESPGDpublisherCB_func(clientGDStatus eventBlockStatus, long eventBlockID, Object ctx) {
		// Garanteed Delivery is not needed;
		return;
	}

	@Override
	public void dfESPpubsubErrorCB_func(clientFailures failure, clientFailureCodes code, Object ctx) {
		Logger.error("ESP Pub/Sub Error-Code: {} occured. {}", code.name(), failure.name());
		switch (failure) {
		case pubsubFail_APIFAIL:
			Logger.error("Client subscription API error with code " + code);
			break;
		case pubsubFail_THREADFAIL:
			Logger.error("Client subscription thread error with code " + code);
			break;
		case pubsubFail_SERVERDISCONNECT:
			Logger.error("Server disconnect");
		}
		return;
	}

	@Override
	public void dfESPsubscriberCB_func(dfESPeventblock eventBlock, dfESPschema schema, Object ctx) {
		int eventBlockSize = eventBlock.getSize();

		
		Logger.debug("Received New Event-Block with {} Events", eventBlockSize);
		
		for (int i = 0; i < eventBlockSize; i++) {
			// Create RDM Request
			RDMEngine rdmEngine = ((DSAdapterContext) ctx).getRtdmEngine();
			try {
				rdmEngine.invokeRdm(((DSAdapterContext) ctx).getRtdmEventName(), eventBlock.getEvent(i), schema, RDMEngine.getCorrelationId(), RDMEngine.getTimezone());
			
			} catch (RuntimeException e) {
				Logger.warn("Failed to pass event {} to RTDN because of {}",i, e.getCause().getLocalizedMessage());
			
			} catch (Exception e) {
				Logger.info("Failed to pass event {} to RTDN because of {}", i, e.getLocalizedMessage());
				//Logger.error(e);
			}
			
			
			
			
		}
		
	}
	
	@Deprecated
	public List<RDMParameter<?>> generateRdmRequest(dfESPevent event, dfESPschema schema) throws dfESPException {
		List<RDMParameter<?>> rdmParameter = new ArrayList<RDMParameter<?>>();
		int rdmParameterCount = schema.getNumFields();
		
		for(int i = 0; i < rdmParameterCount; i++) {
			String parameterName = schema.getNames().get(i);
			datavar parameterData = event.copyByExtID(schema, i);
			RDMParameter<?> rdmParam = null;
			
			switch (parameterData.getType()) {
			case TIMESTAMP:
			case DATETIME:
				// treat as Date
				rdmParam = new RDMParameter<Date>(parameterName, Datatype.DateTime, (Date) parameterData.getValue());
				break;
			case INT32:
			case INT64:
				// treat as Long.
				rdmParam = new RDMParameter<Long>(parameterName, Datatype.Long, (Long) parameterData.getValue());
				break;
			case DOUBLE:
			case MONEY:
				rdmParam = new RDMParameter<Double>(parameterName, Datatype.Double, (Double) parameterData.getValue());
				break;
			case UTF8STR:
				rdmParam = new RDMParameter<String>(parameterName, Datatype.String, (String) parameterData.getValue());
				break;
			case NULL:
			case PARTIALUPDATE:
			case UNKNOWN:
			default:
				Logger.warn("Event Parameter {} Datatype {} is not supported. The value is not transmitted to RTDM.", parameterName, parameterData.getType());
				break;
			}
			
			
			Logger.debug("Adding RTDM Parameter for {} with Type of {} ", parameterName, rdmParam.getType().name());
			rdmParameter.add(rdmParam);
		}
		
		
		return rdmParameter;
	}

}
