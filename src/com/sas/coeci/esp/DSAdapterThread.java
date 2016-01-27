package com.sas.coeci.esp;

import java.net.UnknownHostException;

import org.pmw.tinylog.Logger;

import com.sas.coeci.esp.rdm.RDMEngine;
import com.sas.esp.api.server.event.EventOpcodes;
import com.sas.esp.api.server.ReferenceIMPL.dfESPeventblock;
import com.sas.esp.api.server.ReferenceIMPL.dfESPschema;
import com.sas.tap.client.SASDSResponse;

public class DSAdapterThread implements Runnable {

	private dfESPeventblock eventBlock;
	private dfESPschema schema;
	private DSAdapterContext ctx;
	private boolean insertOnly;

	public DSAdapterThread(dfESPeventblock eventBlock, dfESPschema schema, DSAdapterContext ctx) {
		this.eventBlock = eventBlock;
		this.schema = schema;
		this.ctx = ctx;
		String espInsertOnly = ctx.getConfigProperties().getProperty("esp.insertOnly");
		insertOnly = (espInsertOnly != null && espInsertOnly.equalsIgnoreCase("true"));
	}

	@Override
	public void run() {
		int eventBlockSize = eventBlock.getSize();
		Logger.info("Executing thread to process new event block {} with {} events", eventBlock.hashCode(), eventBlockSize);
		long startTime, elapsedTime, responseTime;
		for (int i = 0; i < eventBlockSize; i++) {
			startTime = System.currentTimeMillis();
			// Create RDM Request
			RDMEngine rdmEngine = ctx.getRtdmEngine();
			try {
				Logger.info("Passing ESP Event {} [Block {}] to RTDM", i, eventBlock.hashCode());

				// check if event should be ignored:
				if (insertOnly && eventBlock.getEvent(i).getOpcode() != EventOpcodes.eo_INSERT) {
					Logger.debug("Event is not passed to RTDM because it is not an INSERT event.");
				}
				else
				{
					SASDSResponse response = rdmEngine.invokeRdm(ctx.getRtdmEventName(), eventBlock.getEvent(i), schema);
					responseTime = response.getEndTime().getMillisecond() - response.getStartTime().getMillisecond();
					elapsedTime = System.currentTimeMillis() - startTime;

					Logger.debug("Received response {} from RTDM after {} milliseconds. cpu time: {} ms", response.getName(), responseTime, elapsedTime);
				}
			} catch (Exception e) {
				String explaination = "Please check the RTDM log.";

				if (e.getCause() instanceof UnknownHostException) {
					explaination = "Cannot connect to " + rdmEngine;
				} else if (e.getCause() != null && e.getCause().getMessage().startsWith("Status code received: 500 Internal Server Error")) {
					explaination = "Please check the event name and variable types.";
				}

				Logger.warn("Failed to pass ESP Event {} [Block {}] to RTDM. {}", i, eventBlock.hashCode(), explaination);
				Logger.trace(e);
			}
		}
	}
}
