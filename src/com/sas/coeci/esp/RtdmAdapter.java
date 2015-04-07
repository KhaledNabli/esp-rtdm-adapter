package com.sas.coeci.esp;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;

import com.sas.coeci.esp.rdm.RDMEngine;
import com.sas.coeci.esp.rdm.RDMParameter;
import com.sas.coeci.esp.rdm.RDMParameter.Datatype;
import com.sas.esp.api.pubsub.clientCallbacks;
import com.sas.esp.api.pubsub.clientFailureCodes;
import com.sas.esp.api.pubsub.clientFailures;
import com.sas.esp.api.pubsub.clientGDStatus;
import com.sas.esp.api.pubsub.dfESPclient;
import com.sas.esp.api.pubsub.dfESPclientHandler;
/* These import files are needed for all subscribing code. */
import com.sas.esp.api.server.event.EventOpcodes;
import com.sas.esp.api.server.ReferenceIMPL.dfESPevent;
import com.sas.esp.api.server.ReferenceIMPL.dfESPeventblock;
import com.sas.esp.api.server.ReferenceIMPL.dfESPschema;
import com.sas.tap.client.SASDSResponse;

public class RtdmAdapter {

	static private boolean nonBusyWait = true;
	
	static private String rtdmHost = "sasbap.demo.sas.com";
	static private int rtdmPort = 8680;
	static private String espWindowUrl = "dfESP://localhost:55555/project/contQuery/activeUsers?snapshot=true";
	static private String rtdmEventName = "ESP_Twitter_Event";
	
	static private RDMEngine rtdmEngine;
	static private String espWindowSchema;
	
	static private Map<String, String> parseEventCsv() {
		
		
		return null;
	}

	
	



	
	
	
	
	
	
	
	
	
	private static clientCallbacks clientCbListener = new clientCallbacks() {

		private RDMEngine myRdmEngine = new RDMEngine(rtdmHost, rtdmPort);

		/*
		 * public void setRDMEngine(RDMEngine rdmInstance) { myRdmEngine =
		 * rdmInstance; }
		 */
		/*
		 * We need to define a subscribe method which will get called when new
		 * events are published from the server via the pub/sub API. This method
		 * gets an eventblock, the schema of the event block for processing
		 * purposes, and an optional user context pointer supplied by the call
		 * to start(). For this example we are just going to write the event as
		 * CSV to System.err.
		 */
		public void dfESPsubscriberCB_func(dfESPeventblock eventBlock, dfESPschema schema, Object ctx) {
			dfESPevent event;
			int eventCnt = eventBlock.getSize();

			for (int eventIndx = 0; eventIndx < eventCnt; eventIndx++) {
				/* Get the event out of the event block. */
				event = eventBlock.getEvent(eventIndx);
				try {
					/*
					 * Convert from binary to CSV using the schema and print to
					 * System.err.
					 */
					String eventStr = event.toStringCSV(schema, true, false);
					// System.out.println(eventStr);
					String[] eventStrFields = eventStr.split(",");
					// TODO find a better way.
					String userName = eventStrFields[1].substring(2);
					String tweetCnt = eventStrFields[3];
					String tweetFollower = eventStrFields[2];

					if (myRdmEngine != null) {
						System.out.println("\n\nForward Event to RTDM for User: " + userName + ", who tweeted " + tweetCnt + " time(s) to " + tweetFollower
								+ " follower(s)!");
						List<RDMParameter> parameterList = new ArrayList<RDMParameter>();
						parameterList.add(new RDMParameter("tw_name", Datatype.String, userName));
						parameterList.add(new RDMParameter("tw_counts", Datatype.Integer, tweetCnt));
						parameterList.add(new RDMParameter("tw_followers", Datatype.Integer, tweetFollower));

						SASDSResponse response = myRdmEngine.invokeRdm(rtdmEventName, parameterList);
						
						
						
						if (response.getString("customer_name") != null)
							System.out.println("RTDM Send SMS to: " + response.getString("customer_name") + " on his mobile "
									+ response.getString("mobile_number"));
					}

				} catch (Exception e) {
					System.err.println("event.toString() failed");
					e.printStackTrace();
					return;
				}
				if (event.getOpcode() == EventOpcodes.eo_UPDATEBLOCK) {
					++eventIndx; /* skip the old record in the update block */
				}
			}
		}

		/*
		 * We also define a callback function for subscription failures given we
		 * may want to try to reconnect/recover, but in this example we will
		 * just print out some error information and release the non-busy wait
		 * set below so the main in program can end. The cbf has an optional
		 * context pointer for sharing state across calls or passing state into
		 * calls.
		 */
		public void dfESPpubsubErrorCB_func(clientFailures failure, clientFailureCodes code, Object ctx) {
			switch (failure) {
			case pubsubFail_APIFAIL:
				System.err.println("Client subscription API error with code " + code);
				break;
			case pubsubFail_THREADFAIL:
				System.err.println("Client subscription thread error with code " + code);
				break;
			case pubsubFail_SERVERDISCONNECT:
				System.err.println("Server disconnect");
			}
			/* Release the busy wait which will end the program. */
			nonBusyWait = false;
		}

		/*
		 * We need to define a dummy publish method which is only used when
		 * implementing guaranteed delivery.
		 */
		public void dfESPGDpublisherCB_func(clientGDStatus eventBlockStatus, long eventBlockID, Object ctx) {
		}
	};

	public static void main(String[] args) throws IOException {

		String engineUrl = espWindowUrl;

		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		dfESPclientHandler handler = new dfESPclientHandler();
		handler.init(Level.WARNING);

		String schemaUrl = engineUrl.substring(0, engineUrl.indexOf('?')) + "?get=schema";
		ArrayList<String> schemaVector = handler.queryMeta(schemaUrl);
		System.err.println(schemaVector.get(0));

		dfESPclient client = handler.subscriberStart(engineUrl, clientCbListener, 0);

		/* Now make the actual connection to the ESP application or server. */
		if (!handler.connect(client)) {
			System.err.println("connect() failed");
			System.exit(1);
		}
		/* Create a mostly non-busy wait loop. */
		while (nonBusyWait) {
			try {
				Thread.sleep(1000); // sleep for 1000 ms
			} catch (InterruptedException ie) {
				// just continue sleeping
			}
		}

		handler.stop(client, true);
	}
}