package com.sas.coeci.esp;

import java.util.concurrent.Executor;

import org.pmw.tinylog.Logger;

import com.sas.esp.api.pubsub.clientCallbacks;
import com.sas.esp.api.pubsub.clientFailureCodes;
import com.sas.esp.api.pubsub.clientFailures;
import com.sas.esp.api.pubsub.clientGDStatus;
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
		System.exit(1);
	}

	@Override
	public void dfESPsubscriberCB_func(dfESPeventblock eventBlock, dfESPschema schema, Object ctx) {
		Logger.info("Received new event block {} with {} events", eventBlock.hashCode(), eventBlock.getSize());
		Logger.debug("Adding a new thread to thread pool to process event block {}", eventBlock.hashCode());

		Executor executor = ((DSAdapterContext) ctx).getExecutor();
		executor.execute(new DSAdapterThread(eventBlock, schema, (DSAdapterContext) ctx));
		
	}
}
