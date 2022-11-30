package dmapf.agents;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;


import dmapf.constants.ConstantsDefinition;
import dmapf.model.EdgeDetail;
import dmapf.model.Message;
import dmapf.utils.DateUtil;
import dmapf.utils.JGraphTUtil;

/**
 * This class is the moving agent, which has to travel from defined source to destination
 * It will first plan its best path, generate the schedule and transmit reservations 
 * for the same to the Location agent. It awaits response from the location agent. 
 * 
 * If the responses received from all the location agents are consistent with its plan,
 * then it will send an acceptance. 
 * 
 * If the responses received have a change from its original plan, or is inconsistent, then 
 * it will compute its second best plan and compare it with the deviation offered by the location
 * agent. Depending on which is lower cost, it will send a reservation again.
 * 
 * If it has send an acceptance, then it will wait till the deliberation window, for further
 * messages from Location. If within the deliberation window, the Traveller receives a revision, 
 * then it reschedules, sends reservation again and cancels the deliberation window.
 * 
 * A deliberation window is maintained per Traveller. It is initiated when the Traveller receives a 
 * dw-init from all locations for which it has confirmed acceptance. It then sends a dw-tick with every 
 * iteration to those locations, to ensure the reservations. If at any time during a dw, the Traveller
 * receives a reschedule, then it will terminate the dw, and send fresh reserve/reject msgs to relevant
 * locations.
 * @author st
 *
 */
public class Travelers /* implements Runnable */
{
	private int agentId;
	private String strSrcNode; 
	private String strDestNode;
	private static final String HASH = "#";
	private static final String EQUALS = "=";

	public int iVehs;
	public int iVTKM;
	public int iTPP;
	int iAgentsMessagedTo;
	public long lInitialCost;
	public String strInitialSchedule="";

	private int startDate;
	private int endDate;
	public long agentCost=0; 

	JGraphTUtil sUtil ;
	private int iSpeed;
	private int iLength;
	Integer agentStatus = ConstantsDefinition.RESERVE;
	private static final int ALTERNATE = 0;
	private static final int PROPOSED = 1;
	private static final int INVALID = -1;
	private static final int PERIOD_INDEX = 0;

	boolean bWaitImposed = false; //maintains if any wait condition is imposed in received plans.
	boolean bAcceptRxdFromAllLocations = false;

	private int iDWSize;
	boolean bFirstRun = true;
	public static final Logger log = Logger.getLogger(Travelers.class);
	private List<Message> alRxdMsg = new ArrayList<Message>();//list of rxd msgs from all agents
	boolean bFinalisedMessageRxd = false; //maintains whether finalised msg has been received from all locations.
	HashMap<String, String> hmProposedPlan = new HashMap<String, String>(); //"L"edgeId, timeDuration from locations.
	private boolean bOrderingConsistent =true; //maintains if the order of locations in the path has changed because of the received proposals.
	ArrayList<String>alWaitLocations = new ArrayList<String>(); //startTime%endTime#LocationAgentId#Waitduration.
	LinkedHashMap<String, String> hmReservedPlan = new LinkedHashMap<String, String>(); //plan sent for reservation .edgeId, start%end
	ArrayList<String> alRevisedProposal = new ArrayList<String>(); //will maintain the plan with waits included. start%end#locationId
	TreeMap<Integer, ArrayList<String>> mapSchedules ; //this will maintain the sorted schedule received from agents. startdate, start%end#locationId 
	LinkedHashMap<String, String> hmAlternatePlan = new LinkedHashMap<String, String>(); //alternate plan generated .edgeId, start%end
	ArrayList<String> alConstraintEdges = new ArrayList<String>(); //maintains the locationIds which should be excluded in the path. Cumulative list.
	private int iDWCount = 1;
	private int iProperty;

	public Travelers( int agentId, int iDW, int iSendProperty)
	{
		this.agentId = agentId; 
		this.iDWSize = iDW;
		this.iProperty = iSendProperty;

		sUtil = JGraphTUtil.getInstance();
		PropertyConfigurator.configure("resources/config-properties/log4j.properties");

	}

	public int getAgentId() {
		return agentId;
	}

	public void setAgentId(int agentId) {
		this.agentId = agentId;
	}
	/**
	 *  This function will find the best path from the soruce
	 *  to the destination for this agent.
	 */
	public  ArrayList<Message>  generateSchedule()
	{
		log.debug("Agent:"+this.agentId+" generateSchedule");
		//The path is a space-separated string with node-id,followed by edge-id. The last
		//entry in the string is the length of the path.
		String path = sUtil.getPath(strSrcNode, strDestNode, null);
		hmReservedPlan.clear();
		hmReservedPlan.putAll(schedule(path, INVALID));

		//Send reserve request to the locations.
		computeInitCost();
		return transmitMessage();

	}

	private void computeInitCost() 
	{
		long lCost = 0;


		boolean bLatestSet = false;
		int calLatest = 0;

		Iterator<String>iterPlan = hmReservedPlan.keySet().iterator();
		while(iterPlan.hasNext())
		{

			try
			{
				String loc = iterPlan.next();
				String strSchedule = hmReservedPlan.get(loc);

				String timeDuration = strSchedule.split(ConstantsDefinition.MSGBODY_DELIMITER)[0];
				int calCurrentEnd = Integer.parseInt(timeDuration.split(ConstantsDefinition.TIMEDURATION_DELIMITER)[1]);
				strInitialSchedule=strInitialSchedule.concat(",")+loc+EQUALS+timeDuration;
				if(!bLatestSet)
				{
					calLatest = Integer.parseInt(timeDuration.split(ConstantsDefinition.TIMEDURATION_DELIMITER)[1]);
					bLatestSet=true;
				}
				else
				{
					if(calCurrentEnd > calLatest)
						calLatest = calCurrentEnd; 
				}
			}catch(Exception e)
			{
				e.printStackTrace();
			}
		}
		lCost = calLatest - this.startDate;

		lInitialCost = lCost;
		log.debug("Agent:"+this.agentId+ " initialCost:"+lInitialCost +" initialSchedule"+strInitialSchedule);


	}

	/**
	 * This function receives the path for each convoy agent and will generate the schedule for it.
	 * String with the space separated list of node-arc-node. Last entry is the "double" distance length.
	 *  @param result:Key:edgeId , Value: startTimeTIMEDURATIONDELIMITERendTime
	 */
	public LinkedHashMap<String, String> schedule(String path, int startDate)
	{
		int indexOfDistance = path.lastIndexOf(ConstantsDefinition.PATH_DELIMITOR); //This will mark the pos where the distance is given in the string.
		LinkedHashMap<String,String> tmpplan = new LinkedHashMap<String, String>();
		if(indexOfDistance == -1)
		{
			log.error("distance not found in path "+path+ "of agent "+agentId);
			System.exit(1);
		}

		String sPath = path.substring(0, indexOfDistance); //The path for the convoy is from the starting index value till the index where distance is defined.

		log.debug(" dist:"+path.substring(indexOfDistance));


		int scheduledStartDateTime ;
		if(startDate == INVALID)
			scheduledStartDateTime = this.startDate;
		else
			scheduledStartDateTime = startDate;

		String[] splitPath = sPath.split(ConstantsDefinition.PATH_DELIMITOR);


		//the path contains the nodeId followed by ArcIds. ArcIds are on the odd index positions
		for(int iSplitPathIndex = 1; iSplitPathIndex < splitPath.length ; iSplitPathIndex+=2)
		{
			//for the scheduled start date and time and given path compute the time taken to
			//	boolean bSameDirection = false;
			if(iSplitPathIndex != splitPath.length - 1)
			{
				String road = splitPath[iSplitPathIndex];

				//Check for directionality
				EdgeDetail edge = sUtil.getEdgeDetail(road);
				//log.debug("edgeDetail "+edge.getEdgeDetailString());
				/*if(edge.getStartV().getId().equalsIgnoreCase(headNodeId))
					bSameDirection = true;
				 */
				//Compute time to move on the arc.

				int iSpeedOfTravel = getMinSpeed(splitPath, iSplitPathIndex);
				double distance = edge.getDistance()*1000;

				int iTimeToTravelArc = (int)((distance / iSpeedOfTravel) * ConstantsDefinition.MINS_IN_1HR);
				if(iTimeToTravelArc<1)
					iTimeToTravelArc = 1;
				log.debug("Agent:"+this.agentId+" speed:"+iSpeedOfTravel+" dist:"+distance+" timeToTravel:"+iTimeToTravelArc+ " iTPP:"+iTPP);

				endDate = scheduledStartDateTime + iTimeToTravelArc + iTPP;
				String timePeriod = String.valueOf(scheduledStartDateTime).concat(ConstantsDefinition.TIMEDURATION_DELIMITER.concat(String.valueOf(endDate)));
				scheduledStartDateTime += iTimeToTravelArc;

				//Add the time for the tail of the convoy to clear the arc.
				tmpplan.put(edge.getId(),timePeriod);
			}

		}//end for
		log.debug("Agent:"+this.agentId+" plan:"+tmpplan);
		return tmpplan;
	}
	/** This function will find the edges occupied by the agent currently. This is done
	 * by comparing the length of the agent to the length of the edges traversed, moving 
	 * backward from iSplitPathIndex. 
	 * The minimum speed returned will be the minimum of the speeds of the edges occupied,
	 * edge to be entered into(iSplitPathIndex) and the speed of the agent itself.
	 * 
	 * @param splitPath : splitArray of the edge names in the path
	 * @param iSplitPathIndex: index on splitPath of the edge on which the agent is to enter
	 * @return Speed: min speed
	 */
	private int getMinSpeed(String[] splitPath, int iSplitPathIndex) {
		int iMinSpeed = this.getSpeed();
		double dTotalEdgeLength = 0;

		//Find the edges occupied. The edges are on alternate index positions
		for(int i=iSplitPathIndex-2; i>=0; i-=2)
		{
			EdgeDetail edge = sUtil.getEdgeDetail(splitPath[i]);
			dTotalEdgeLength = edge.getDistance();


			if(dTotalEdgeLength <= this.iLength)
			{
				if(iMinSpeed > edge.getSpeed())
					iMinSpeed = edge.getSpeed();

			}
			else 
				break;
		}//end for

		return iMinSpeed;
	}

	/**
	 *  This function will receive the msg from the postoffice agent.
	 *  The rxd msgs can have the following status: PROPOSE, DW_INIT, ...
	 *  
	 * @param arrayList
	 */
	public void receive(ArrayList<Message> alRxdMsgFromPO)
	{
		log.debug("Traveller Agent:"+agentId);
		alRxdMsg.clear();

		//	alRxdMsg =  Collections.synchronizedList(alRxdMsg);

		//synchronized (alRxdMsg) 
		{
			alRxdMsg.addAll(alRxdMsgFromPO);
			log.debug("Traveller"+agentId+": Msg List:"+alRxdMsg);
		}

	}

	public ArrayList<Message> processMsg()
	{
		log.debug("Agent:"+this.agentId+" processMsg. DWCount:"+iDWCount);
		if(bFinalisedMessageRxd)
		{
			agentStatus = ConstantsDefinition.FINALIZE;
			return transmitResponse();
		}
		else if(iDWCount>iDWSize)
		{
			agentStatus = ConstantsDefinition.TERMINATE;
			return transmitResponse();
		}
		else
		{
			bWaitImposed = false;
			alWaitLocations.clear();
			return checkConsistency();
		}
	}
	/**
	 * This function will check if the msgs received from the different
	 * edges will make a consistent plan, with respect to the constraints 
	 * imposed. If yes then check if alternate plan with lesser cost exists.
	 * If no, give a confirmation for this plan, if yes, then send a reservation
	 * for the smaller plan.
	 * 
	 * @param alRxdMsg: List of rxd msgs from all edges.
	 */
	private ArrayList<Message>  checkConsistency()
	{
		//move the proposes, DWs to a common structure
		log.debug("Traveller agent"+this.agentId+" checking consistency ");

		//segregate based on whether it is proposes/accept etc
		divideBasedOnMsgType();


		//check based on the recived responses, whether the plan
		//is consistent or not
		bOrderingConsistent = true;
		boolean bPlanConsistent = checkPlanConsistent();

		if(bPlanConsistent)
		{
			//update hmReserved. Iterate thru the rxd msg and update the timing of hmReserved 
			for(Message msg:alRxdMsg)
			{	
				if(!hmReservedPlan.containsKey(msg.getSender().replace(ConstantsDefinition.LOCATION, "")))
					log.error("Reserved plan does not contain entry for:" +msg.getSender().replace(ConstantsDefinition.LOCATION, ""));

				String strDuration = hmReservedPlan.get(msg.getSender().replace(ConstantsDefinition.LOCATION, ""));
				String newDuration = msg.getBody().split(ConstantsDefinition.MSGBODY_DELIMITER)[1];
				hmReservedPlan.put(msg.getSender().replace(ConstantsDefinition.LOCATION, ""), newDuration);
				log.debug("Agent:"+this.agentId+ " Updating hmReservedPlan for location"+msg.getSender().replace(ConstantsDefinition.LOCATION, ""));
				log.debug("Agent:"+this.agentId+ " updated reserved plan from "+strDuration +" to "+newDuration);

			}
			return transmitResponse();
		}
		else //if(bWaitImposed || !bOrderingConsistent)
		{
			//if wait has been imposed or ordering is not consistent, check if alternate plan possible else agree to the plan
			//if plan is not consistent, generate alternate plan.
			log.debug("Agent:"+this.agentId+ " Wait "+bWaitImposed +" consistent:"+bPlanConsistent+ "ordering:"+bOrderingConsistent);

			generateConstraints();
			scheduleAlternate();
			return rescheduleAndTransmit();	
		}

	}
	/**
	 *  Compare the received proposals with the alternate agent plan. If alternate is of 
	 *  lower cost, the transmit reservation msgs again. Send reject to locations which do 
	 *  not form part of this plan but were there earlier. 
	 *  If alternate is of higher cost, accept the proposal and send the accept. If some of 
	 *  the locations have to be reserved again, send reserve msg.
	 */
	private ArrayList<Message>  rescheduleAndTransmit() 
	{
		log.debug("Agent:"+this.agentId+ " rescheduleD"+mapSchedules);
		ArrayList<Message> alMessages = new ArrayList<Message>(); 
		long lCostProposed = computeCostProposed();

		if(bOrderingConsistent)
		{

			if(!hmAlternatePlan.isEmpty())
			{
				long lCostAlternate = computeCostAlternate();
				if(lCostAlternate < lCostProposed)
				{
					alMessages.addAll(resetReservedPlan(ALTERNATE));
				}
				else
				{
					alMessages.addAll(resetReservedPlan(PROPOSED));
				}
			}
			else
				alMessages.addAll(resetReservedPlan(PROPOSED));


		}
		else //in this case, only the alternate should be sent
		{
			log.debug("Agent:"+this.agentId+ " Ordering inconsistent. Send alternate plan");
			if(!hmAlternatePlan.isEmpty())
			{
				long lCostAlternate = computeCostAlternate();
				alMessages.addAll(resetReservedPlan(ALTERNATE));
			}
			else
			{
				log.debug("Agent:"+this.agentId+ " Alternate plan also not available??????.Sending proposed with wait");
				alMessages.addAll(resetReservedPlan(PROPOSED));
			}
		}
		//Update hmReserved, if rejects are there, remove the entry from the hmReservedPlan
		for(Message msg:alMessages)
		{	
			String strDuration = hmReservedPlan.get(msg.getReceiver().replace(ConstantsDefinition.LOCATION, ""));
			String newDuration = msg.getBody().split(ConstantsDefinition.MSGBODY_DELIMITER)[1];
			String status = msg.getBody().split(ConstantsDefinition.MSGBODY_DELIMITER)[0];
			if(status.equalsIgnoreCase(ConstantsDefinition.REJECT.toString()))
			{
				hmReservedPlan.remove(msg.getReceiver().replace(ConstantsDefinition.LOCATION, ""));
				log.debug("Agent:"+this.agentId+  " removed "+msg.getReceiver().replace(ConstantsDefinition.LOCATION, ""));
			}
			else
			{
				hmReservedPlan.put(msg.getReceiver().replace(ConstantsDefinition.LOCATION, ""), newDuration);
				log.debug("Agent:"+this.agentId+ " added "+msg.getReceiver().replace(ConstantsDefinition.LOCATION, ""));
			}
			log.debug("Agent:"+this.agentId+ " updated reserved plan from "+strDuration +" to "+newDuration+ "for "+msg.getReceiver().replace(ConstantsDefinition.LOCATION, ""));
		}

		//Check if there are older entries in hmReserved which should now be removed.
		Iterator<String> iterKey = hmReservedPlan.keySet().iterator();
		ArrayList<String> alDeletes = new ArrayList<String>();
		while(iterKey.hasNext())
		{
			String strLocation = iterKey.next();
			boolean bFound = false;
			for(Message msg:alMessages)
			{
				if(msg.getReceiver().equalsIgnoreCase(ConstantsDefinition.LOCATION.concat(strLocation)))
				{
					bFound = true;
					break;
				}
			}
			if(bFound == false)
				alDeletes.add(strLocation);
		}

		//remove the unused entries from hmReserved
		log.debug("Agent:"+this.agentId+ " Removing entries for "+alDeletes+" from hmReservedPlan");
		for(String strDelete:alDeletes)
			hmReservedPlan.remove(strDelete);
		log.debug("Agent:"+this.agentId+ " hmReservedPlan:"+hmReservedPlan);
		if(iDWCount <= iDWSize)
		{
			//iDWCount++;
			return sendMessage(alMessages);
		}
		else
		{
			agentStatus = ConstantsDefinition.TERMINATE;
			log.debug("Agent:"+this.agentId+" agentStatus"+agentStatus+" iDWCount:"+iDWCount);
			return null;
		}

	}//end function


	/**
	 * This function will reset the hmReservedPlan with either the
	 * identified alternative plan or the proposed plan from the locations
	 * with waits. The choice is made depending on the value of resetType
	 * resetType = ALTERNATE : update with the alternate plan
	 * resetType = PROPOSED: update with the proposed plan.
	 * In both cases iterate through the hmProposed, check the msg type,
	 * if the msg to be sent is same as the proposed, update the status type,
	 * else reset the status type to RESERVE. Finally if there are locations
	 * to which msgs are not being sent at all, but from which msgs have been
	 * received, add msgs of REJECT to them.
	 * Since the hmProposedPlan contains the entries as received from the locations,
	 * they may not be in theright order, hence iterate on the the hmReserved, get the
	 * edge ids in order from it, then get the corresponding entry from the hmPropsedPlan
	 * @param resetType: ALTERNATE/PROPOSED
	 */
	private ArrayList<Message> resetReservedPlan(int resetType) 
	{
		ArrayList<Message> alMessages = new ArrayList<Message>();
		Iterator<String>iterReserved = hmReservedPlan.keySet().iterator();

		while(iterReserved.hasNext())
		{
			String strLocation = iterReserved.next(); //locationId without L
			String strLocationId = ConstantsDefinition.LOCATION.concat(strLocation); //LlocationId
			if(resetType == ALTERNATE)
			{
				if(hmAlternatePlan.containsKey(strLocation))
				{
					Message msg = new Message();
					msg.setReceiver(ConstantsDefinition.LOCATION.concat(strLocation));
					String senderId = ConstantsDefinition.TRAVELLER.concat(String.valueOf(this.agentId));
					switch(iProperty)
					{
					case ConstantsDefinition.LOCATION_SCHEDULE_LENGTH:
						senderId = senderId.concat(ConstantsDefinition.SENDER_DELIMITER).concat(String.valueOf(this.iLength));
						break;
					case ConstantsDefinition.LOCATION_SCHEDULE_SPEED:
						senderId = senderId.concat(ConstantsDefinition.SENDER_DELIMITER).concat(String.valueOf(this.iSpeed));
						break;
					}
					msg.setSender(senderId);
					log.debug("Agent:"+this.agentId+" transmit msg. sender "+msg.getSender());
					
					//msg.setSender(ConstantsDefinition.TRAVELLER.concat(String.valueOf(agentId)));
					String strMsgBody = "";
					String strStatus = ConstantsDefinition.RESERVE.toString();
					String strDuration = hmAlternatePlan.get(strLocation);
					strMsgBody = strStatus.concat(ConstantsDefinition.MSGBODY_DELIMITER)
							.concat(strDuration).concat(ConstantsDefinition.MSGBODY_DELIMITER)
							.concat(String.valueOf(iDWCount));
					//iDWCount++;
					msg.setBody(strMsgBody);
					alMessages.add(msg);
				}
				else //the location is no longer part of the alternate plan. Add a reject message
				{
					Message msg = new Message();
					msg.setReceiver(ConstantsDefinition.LOCATION.concat(strLocation));
					String senderId = ConstantsDefinition.TRAVELLER.concat(String.valueOf(this.agentId));
					switch(iProperty)
					{
					case ConstantsDefinition.LOCATION_SCHEDULE_LENGTH:
						senderId = senderId.concat(ConstantsDefinition.SENDER_DELIMITER).concat(String.valueOf(this.iLength));
						break;
					case ConstantsDefinition.LOCATION_SCHEDULE_SPEED:
						senderId = senderId.concat(ConstantsDefinition.SENDER_DELIMITER).concat(String.valueOf(this.iSpeed));
						break;
					}
					msg.setSender(senderId);
					log.debug("Agent:"+this.agentId+" transmit msg. sender "+msg.getSender());
					
					//msg.setSender(ConstantsDefinition.TRAVELLER.concat(String.valueOf(agentId)));
					String strMsgBody = "";
					String strStatus = ConstantsDefinition.REJECT.toString();
					String strDuration = hmProposedPlan.get(strLocationId);
					strMsgBody = strStatus.concat(ConstantsDefinition.MSGBODY_DELIMITER)
							.concat(strDuration).concat(ConstantsDefinition.MSGBODY_DELIMITER)
							.concat(String.valueOf(iDWCount));
					//iDWCount++;
					msg.setBody(strMsgBody);
					alMessages.add(msg);
				}
			}
			else //resetType == PROPOSED
			{
				//Get the updated timeduration from the alRevisedProposal
				int index = getUpdatedDetails(strLocation);
				if(index!=INVALID)
				{
					String strRevised = alRevisedProposal.get(index);

					Message msg = new Message();
					msg.setReceiver(ConstantsDefinition.LOCATION.concat(strLocation));
					String senderId = ConstantsDefinition.TRAVELLER.concat(String.valueOf(this.agentId));
					switch(iProperty)
					{
					case ConstantsDefinition.LOCATION_SCHEDULE_LENGTH:
						senderId = senderId.concat(ConstantsDefinition.SENDER_DELIMITER).concat(String.valueOf(this.iLength));
						break;
					case ConstantsDefinition.LOCATION_SCHEDULE_SPEED:
						senderId = senderId.concat(ConstantsDefinition.SENDER_DELIMITER).concat(String.valueOf(this.iSpeed));
						break;
					}
					msg.setSender(senderId);
					log.debug("Agent:"+this.agentId+" transmit msg. sender "+msg.getSender());
					
					//msg.setSender(ConstantsDefinition.TRAVELLER.concat(String.valueOf(agentId)));
					String strMsgBody = "";
					String strStatus = ConstantsDefinition.RESERVE.toString();
					String strDuration = strRevised.split(ConstantsDefinition.MSGBODY_DELIMITER)[0];
					strMsgBody = strStatus.concat(ConstantsDefinition.MSGBODY_DELIMITER)
							.concat(strDuration).concat(ConstantsDefinition.MSGBODY_DELIMITER)
							.concat(String.valueOf(iDWCount));
					//	iDWCount++;
					msg.setBody(strMsgBody);
					alMessages.add(msg);
				}//index!=INVALID
				else 
				{
					log.error("Chosen to go with the proposed details, but edgeId "+strLocation+" is not in the alRevisedProposal");
				}
			}
			//update the hmReservedPlan

		}//end while iterProposed

		//Check if there are any additional locations in the alter/revised which
		//are yet to be added to the reserved list. Add and send RESERVE msg to those.
		alMessages.addAll(addRemaining(alMessages, resetType));
		//log.info("Agent:"+agentId+" DW:"+iDWCount);
		iDWCount++;
		return alMessages;
	}
	/**
	 *  This function will add any unadded locations into the alMessages. 
	 *  Depending on the resetType, it will iterate through  hmAlternatePlan or
	 *  alRevisedProposal and check if all the entries there are captured in 
	 *  alMessages. If any entry is missing, then it is added to the alMessages and 
	 *  returned.
	 * @param alMessages :list of messages to be sent as a response
	 * @param resetType: ALTERNATE/PROPOSED
	 * @return alMessage: list of new remaining messages to be added
	 */
	private ArrayList<Message> addRemaining(ArrayList<Message> alMessages, int resetType) 
	{
		ArrayList<Message> alRemainingMsgs = new ArrayList<Message>(); //

		if(resetType == ALTERNATE)
		{
			Iterator<String> iterAlt = hmAlternatePlan.keySet().iterator();
			while(iterAlt.hasNext())
			{
				String edgeId = iterAlt.next();
				boolean bFound  =false;
				for(Message msg:alMessages)
				{
					if(msg.getReceiver().equalsIgnoreCase(ConstantsDefinition.LOCATION.concat(edgeId)))
					{
						bFound = true;
						break;
					}
				}//for i

				if(!bFound) //no entry for the edge in the messages
				{
					log.debug("Agent:"+this.agentId+ " No entry for "+edgeId);
					Message msg = new Message();
					msg.setReceiver(ConstantsDefinition.LOCATION.concat(edgeId));
					String duration = hmAlternatePlan.get(edgeId);
					String senderId = ConstantsDefinition.TRAVELLER.concat(String.valueOf(this.agentId));
					switch(iProperty)
					{
					case ConstantsDefinition.LOCATION_SCHEDULE_LENGTH:
						senderId = senderId.concat(ConstantsDefinition.SENDER_DELIMITER).concat(String.valueOf(this.iLength));
						break;
					case ConstantsDefinition.LOCATION_SCHEDULE_SPEED:
						senderId = senderId.concat(ConstantsDefinition.SENDER_DELIMITER).concat(String.valueOf(this.iSpeed));
						break;
					}
					msg.setSender(senderId);
					log.debug("Agent:"+this.agentId+" transmit msg. sender "+msg.getSender());
					
					//msg.setSender(ConstantsDefinition.TRAVELLER.concat(String.valueOf(agentId)));
					String msgBody = ConstantsDefinition.RESERVE.toString().concat(ConstantsDefinition.MSGBODY_DELIMITER)
							.concat(duration).concat(ConstantsDefinition.MSGBODY_DELIMITER)
							.concat(String.valueOf(iDWCount));
					//	iDWCount++;
					msg.setBody(msgBody);
					log.debug("Agent:"+this.agentId+ " Adding "+msg+" to the remaining list");
					alRemainingMsgs.add(msg);
				}
			}//end while iterAlt
		}//end ALTERNATE
		else //RPOPOSED
		{
			for(String strRevised:alRevisedProposal) //start%end#locationId
			{
				String locationId = strRevised.split(ConstantsDefinition.MSGBODY_DELIMITER)[1];
				String duration = strRevised.split(ConstantsDefinition.MSGBODY_DELIMITER)[0];
				boolean bFound  = false;
				for(Message msg:alMessages)
				{
					if(msg.getReceiver().equalsIgnoreCase(ConstantsDefinition.LOCATION.concat(locationId)))
					{
						bFound = true;
						break;
					}
				}//for i

				if(!bFound) //no entry for the edge in the messages
				{
					log.debug("Agent:"+this.agentId+ " No entry for "+locationId);
					Message msg = new Message();
					msg.setReceiver(locationId);
					String senderId = ConstantsDefinition.TRAVELLER.concat(String.valueOf(this.agentId));
					switch(iProperty)
					{
					case ConstantsDefinition.LOCATION_SCHEDULE_LENGTH:
						senderId = senderId.concat(ConstantsDefinition.SENDER_DELIMITER).concat(String.valueOf(this.iLength));
						break;
					case ConstantsDefinition.LOCATION_SCHEDULE_SPEED:
						senderId = senderId.concat(ConstantsDefinition.SENDER_DELIMITER).concat(String.valueOf(this.iSpeed));
						break;
					}
					msg.setSender(senderId);
					log.debug("Agent:"+this.agentId+" transmit msg. sender "+msg.getSender());
					
					//msg.setSender(ConstantsDefinition.TRAVELLER.concat(String.valueOf(agentId)));
					String msgBody = ConstantsDefinition.RESERVE.toString().concat(ConstantsDefinition.MSGBODY_DELIMITER)
							.concat(duration).concat(ConstantsDefinition.MSGBODY_DELIMITER)
							.concat(String.valueOf(iDWCount));
					//iDWCount++;
					msg.setBody(msgBody);
					log.debug("Agent:"+this.agentId+ " Adding "+msg+" to the remaining list");
					alRemainingMsgs.add(msg);
				}
			}//end for revisedProposal
		}//end else PROPOSED
		//	iDWCount++;
		return alRemainingMsgs;
	}

	/** This function will look into alRevisedProposal for strLocationId
	 * and if it is found will return the index position else will return IVALID
	 *  
	 */
	private int getUpdatedDetails(String strLocationId) 
	{
		log.debug("Agent:"+this.agentId+ " Searching for strLocationId "+strLocationId+" in "+alRevisedProposal);
		for(String strDetails:alRevisedProposal)
		{
			if (strDetails.split(ConstantsDefinition.MSGBODY_DELIMITER)[1].equalsIgnoreCase(strLocationId))
				return alRevisedProposal.indexOf(strDetails);

		}
		return INVALID;
	}
	/** This function will compute the cost of the alternate plan, which is maintained
	 *  in hmAlternate. Get the starting time of first location
	 *  and ending time of last location and diff of the two is the cost.
	 * 
	 * @return long cost of the laternate plan.
	 */
	private long computeCostAlternate() {

		boolean bFirst =true;
		Integer beginningDate = -1;
		Integer endingDate = -1;

		Iterator<String> iterAlter = hmAlternatePlan.keySet().iterator();
		while(iterAlter.hasNext())
		{
			String strLocation  = iterAlter.next();
			String strDuration = hmAlternatePlan.get(strLocation);// 
			log.debug("Agent:"+this.agentId+" strLoc:"+strLocation+" strDuration:"+strDuration);
			if(bFirst)
			{
				bFirst = false;
				try
				{
					beginningDate = Integer.parseInt(strDuration.split(ConstantsDefinition.TIMEDURATION_DELIMITER)[0]);
					endingDate = Integer.parseInt(strDuration.split(ConstantsDefinition.TIMEDURATION_DELIMITER)[1]);
				}
				catch(Exception e)
				{
					log.error("Couldnt parse date"+strDuration.split(ConstantsDefinition.TIMEDURATION_DELIMITER)[0]);
				}
			}//end if first
			else //check if it is the last entry
			{
				if(!iterAlter.hasNext())
				{
					try
					{
						endingDate = Integer.parseInt(strDuration.split(ConstantsDefinition.TIMEDURATION_DELIMITER)[1]);
					}
					catch(Exception e)
					{
						log.error("Couldnt parse date"+strDuration.split(ConstantsDefinition.TIMEDURATION_DELIMITER)[1]);
					}
				}
			}
		}//end iterAlternate
		log.debug("Agent:"+this.agentId+ "alternate ending date:" +endingDate+ "starting date:"+ beginningDate);

		long lCostAlternate = endingDate- startDate;// beginningDate;	
		log.debug("Agent:"+this.agentId+ " Compute cost alternate:" +lCostAlternate+" hmAlternatePlan:"+hmAlternatePlan);
		return lCostAlternate;
	}

	/**
	 *  This function will compute the cost of the proposed plan, which is maintained
	 *  in sorted order in mapSchedules. There is a need for this function, because 
	 *  in the proposed plan, because of the waits, there will be need to shift the 
	 *  subsequent locations in accordance with the wait introduced. hence the cost 
	 *  should be computed only afer the relevant shiftin has been done. However, since 
	 *  the ordering is consistent in the proposed plan, we can just shift the succeding 
	 *  locations in the plan.
	 * @return
	 */
	private long computeCostProposed() 
	{
		// Iterate through the mapSchedules and using the information in the alWaits
		// get the waiting duration introduced and percolate the wait down the schedule.
		// If in one of the successive location, another wait is introduced, go for the 
		//maximal wait time, computed accordingly.
		long lCost = 0;
		//ArrayList<String> alRevisedProposal = new ArrayList<String>(); //will maintain the plan with waits included.
		alRevisedProposal.clear();
		alRevisedProposal.addAll(updateWaitsToProposedPlan());

		for(String strRevised:alRevisedProposal)
		{
			String timeDuration = strRevised.split(ConstantsDefinition.MSGBODY_DELIMITER)[0];
			lCost+=(DateUtil.getDuration(timeDuration, TimeUnit.MINUTES)-iTPP);
		}
		lCost+=iTPP;
		//If there is wait introduced in the start time itself then it will not be counted in the cost. 
		//So check for the condition and add the additional wait time.
		int revisedStartDate = Integer.parseInt(alRevisedProposal.get(0).split(ConstantsDefinition.MSGBODY_DELIMITER)[0].split(ConstantsDefinition.TIMEDURATION_DELIMITER)[0]);
		if(revisedStartDate > startDate)
			lCost+= (revisedStartDate - startDate);
		
		log.debug("startTime:"+startDate+" revised start: "+alRevisedProposal.get(0).split(ConstantsDefinition.MSGBODY_DELIMITER)[0].split(ConstantsDefinition.TIMEDURATION_DELIMITER)[0]);
		log.debug("Agent:"+this.agentId+ " Compute cost proposed:"+lCost+" proposed:"+alRevisedProposal);
		return lCost;
	}

	/**
	 *  This function will update the proposed plans received from the location
	 *  and incorporate the wait delay into the plans and revise it.
	 *  Iterate thru the hmReservedPlan, check if it is in the waitlist,
	 *  if so update the wait time. The order of the plan should be maintained
	 *  as per the reserved plan with the waits incorporated
	 * @return al of revised plans in sequence; each String contains start%end#locationId
	 */
	private ArrayList<String> updateWaitsToProposedPlan() 
	{
		SimpleDateFormat sdf = new SimpleDateFormat(ConstantsDefinition.DATE_FORMAT);

		ArrayList<String> altmpRevisedProposal = new ArrayList<String>(); //will maintain the plan with waits included. start%end#LocationId
		Iterator<String>iterKey = hmReservedPlan.keySet().iterator(); //edgeId,start%end
		log.debug("Agent:"+this.agentId+ " hmReservedPlan:"+hmReservedPlan.keySet());
		int iTrickledDownWait = 0;
		while(iterKey.hasNext())
		{
			String nextLocation = iterKey.next();
			log.debug("Agent:"+this.agentId+ " nextLocation:"+nextLocation);
			//Iterate thru the waits to see if the edge has a wait imposed on it.
			//If yes, check if wait duration is consistent with the trickled-down wait.
			//If yes, then just add the proposed timeduration from the wait list as the revisedplan
			//If no: if wait-duration > trickled-down wait, add proposed timeduration as is, update the trickled-down wait
			//If no: if wait-duration < trickled-down wait, add the remaining wait duration and revise the proposed
			//If no, ie edge doesnt have a wait imposed on it.
			//If there is a trickled-down wait, revise the proposed plan
			//If there is no trickled-down wait, add it as is.

			int iWaitIndex = locationHasWait(ConstantsDefinition.LOCATION.concat(nextLocation));
			log.debug("Agent:"+this.agentId+ " location has wait <<"+iWaitIndex);
			if(iWaitIndex != INVALID) //location has wait
			{
				String strWaitEntry = alWaitLocations.get(iWaitIndex); //startTime%endTime#LocationAgentId#Waitduration.
				log.debug("Agent:"+this.agentId+ " wait entry:" +strWaitEntry);
				int iWaitDuration = Integer.parseInt(strWaitEntry.split(ConstantsDefinition.MSGBODY_DELIMITER)[2]);

				if(iWaitDuration >= iTrickledDownWait)
				{
					String strRevisedProposal = strWaitEntry.substring(0,strWaitEntry.indexOf(ConstantsDefinition.MSGBODY_DELIMITER)); //remove the LlocationId and wait duration from the entry
					strRevisedProposal = strRevisedProposal.concat(ConstantsDefinition.MSGBODY_DELIMITER).concat(nextLocation); //remove the wait duration from the entry
					altmpRevisedProposal.add(strRevisedProposal);
					log.debug("Agent:"+this.agentId+ " alRevisedProposal: "+altmpRevisedProposal);

					iTrickledDownWait = iWaitDuration;
				}
				else //wait duration < trickled-down wait
				{
					try
					{
						int iAdditionalWait = iTrickledDownWait - iWaitDuration;
						Integer calStartDate = Integer.parseInt(strWaitEntry.substring(0,strWaitEntry.indexOf(ConstantsDefinition.TIMEDURATION_DELIMITER)));
						calStartDate += iAdditionalWait;

						Integer calEndDate = Integer.parseInt(strWaitEntry.substring(strWaitEntry.indexOf(ConstantsDefinition.TIMEDURATION_DELIMITER)+1,strWaitEntry.indexOf(ConstantsDefinition.MSGBODY_DELIMITER)));
						calEndDate += iAdditionalWait;


						String strRevisedProposal = String.valueOf(calStartDate).concat(ConstantsDefinition.TIMEDURATION_DELIMITER)
								.concat(String.valueOf(calEndDate)).concat(ConstantsDefinition.MSGBODY_DELIMITER)
								.concat(nextLocation);

						altmpRevisedProposal.add(strRevisedProposal);
						log.debug("Agent:"+this.agentId+ " alRevisedProposal: "+altmpRevisedProposal);
					}
					catch(Exception e)
					{
						log.error("Agent:"+this.agentId+ " Error parsing "+strWaitEntry.substring(0,strWaitEntry.indexOf(ConstantsDefinition.TIMEDURATION_DELIMITER))+" or "+strWaitEntry.substring(strWaitEntry.indexOf(ConstantsDefinition.TIMEDURATION_DELIMITER)+1,strWaitEntry.indexOf(ConstantsDefinition.MSGBODY_DELIMITER)));
					}
				}//end else 
			}//end if wait
			else //no wait
			{
				String strWaitEntry = hmReservedPlan.get(nextLocation); //startTime%endTime

				if(iTrickledDownWait > 0)
				{
					try
					{
						int iAdditionalWait = iTrickledDownWait ;
						Integer calStartDate = Integer.parseInt(strWaitEntry.substring(0,strWaitEntry.indexOf(ConstantsDefinition.TIMEDURATION_DELIMITER)));
						calStartDate += iAdditionalWait;

						Integer calEndDate = Integer.parseInt(strWaitEntry.substring(strWaitEntry.indexOf(ConstantsDefinition.TIMEDURATION_DELIMITER)+1,strWaitEntry.length()));
						calEndDate += iAdditionalWait;

						String strRevisedProposal =String.valueOf(calStartDate).concat(ConstantsDefinition.TIMEDURATION_DELIMITER)
								.concat(String.valueOf(calEndDate)).concat(ConstantsDefinition.MSGBODY_DELIMITER)
								.concat(nextLocation);
						altmpRevisedProposal.add(strRevisedProposal);
						log.debug("Agent:"+this.agentId+ " alRevisedProposal: "+altmpRevisedProposal);
					}
					catch(Exception e)
					{
						//log.error("Agent:"+this.agentId+ " Error parsing "+strWaitEntry.substring(0,strWaitEntry.indexOf(ConstantsDefinition.TIMEDURATION_DELIMITER))+" or "+strWaitEntry.substring(strWaitEntry.indexOf(ConstantsDefinition.TIMEDURATION_DELIMITER)+1,strWaitEntry.length()));
						e.printStackTrace();
					}
				}
				else
				{
					//add the plan as is
					log.debug("Agent:"+this.agentId+ " strWaitEntry:"+strWaitEntry);
					String strRevisedProposal = strWaitEntry;//.substring(0,strWaitEntry.indexOf(ConstantsDefinition.TIMEDURATION_DELIMITER)); //remove the LlocationId and wait duration from the entry
					strRevisedProposal = strRevisedProposal.concat(ConstantsDefinition.MSGBODY_DELIMITER).concat(nextLocation); //remove the wait duration from the entry

					altmpRevisedProposal.add(strRevisedProposal);
					log.debug("Agent:"+this.agentId+ " alRevisedProposal: "+altmpRevisedProposal);

				}
			}

		}
		log.debug(altmpRevisedProposal);
		return altmpRevisedProposal;
	}
	/**
	 * This function will search through the waitList to see if the
	 * nextLocation exists. If yes, it returns the index position
	 * of the entry in the list, if no it returns INVALID
	 * @param nextLocation: location id to be searched for in the list
	 * @return
	 */
	private int locationHasWait(String nextLocation) 
	{
		log.debug("Agent:"+this.agentId+ " location has wait>>"+nextLocation);
		for(String strEntry:alWaitLocations)
		{
			if(strEntry.contains(nextLocation))
				return alWaitLocations.indexOf(strEntry);
		}

		return INVALID;
	}
	/**
	 *  This function will maintain the constraint tree
	 *  and add current constraints from waits or inconsistent locations. 
	 */
	private void generateConstraints() 
	{
		if(bWaitImposed)
		{
			log.debug("Agent:"+this.agentId+ " Wait imposed on locations:"+alWaitLocations);
			for(int i=0; i<alWaitLocations.size(); i++)
			{
				String strDesc = alWaitLocations.get(i); //start%end#locationId
				alConstraintEdges.add(strDesc.split(ConstantsDefinition.MSGBODY_DELIMITER)[1].replace(ConstantsDefinition.LOCATION, ""));
			}
		}
		else
		{
			log.debug("Agent:"+this.agentId+ " Case where wait is not imposed and ordering"+ bOrderingConsistent+". How to define the constraints");
		}
	}
	/**
	 *  This function will take the current list of constraints 
	 *  and find an alternate plan and schedule it.
	 */
	private void scheduleAlternate() 
	{
		String path = sUtil.getPath(strSrcNode, strDestNode, alConstraintEdges);
		log.debug("Agent:"+this.agentId+ " alternate path is"+path+ "with constraints on"+alConstraintEdges);
		hmAlternatePlan.clear();

		if(path!=null)
			hmAlternatePlan.putAll(schedule(path, INVALID));

	}
	/**
	 *  This function will check if the plan created by the 
	 *  responses from the locations is consistent. Consistency will be
	 *  checked if a) only wait has been introduced, but the temporal sequencing of
	 *  the locations remain unaffected. b) wait has been introduced and temporal
	 *  sequencing of the location does not give valid path
	 *  The proposed plans are maintained in the hmProposedPlan.
	 *  
	 * @return true if ordering is consistent and no waits. else false;
	 */
	private boolean checkPlanConsistent() 
	{
		log.debug("Agent:"+this.agentId+ " CheckPlanConsistent>>");

		if(hmProposedPlan.isEmpty() )
		{
			log.error("Agent:"+this.agentId+ " Proposed plan is empty");
			return false;
		}
		else
		{
			SimpleDateFormat sdf = new SimpleDateFormat(ConstantsDefinition.DATE_FORMAT);

			ArrayList<String> alProposed = new ArrayList<String>();
			Iterator<String>iterKey = hmProposedPlan.keySet().iterator();
			while(iterKey.hasNext())
			{
				String loc = iterKey.next();
				alProposed.add(hmProposedPlan.get(loc).concat(HASH).concat(loc));
			}//end while

			//sort the proposals by the starting time.
			mapSchedules = new TreeMap<Integer, ArrayList<String>>();//startDate, startTime%endTime#LocationAgentId. 
			mapSchedules.putAll(sortByStartingTime(alProposed));

			bOrderingConsistent = checkOrderingConsistency();

			checkWait();

			return (bOrderingConsistent && !bWaitImposed);


		}
	}

	/**
	 *  This function will check if waits have been introduced into the plan. 
	 *  It will compare with the duration of each location in the hmReserved with
	 *  what has been proposed by the locations themselves. The propsed time-duration
	 *  should always be after the reserved time,; it cannot be before the reserved time.
	 *  if there is difference between the two (proposed - reserved), it should be positive
	 *  and set as the wait time and the proposed edge should be added to the list.
	 *  mapSchedules is the sorted list of proposed schedules,
	 *  hmReserved is the list of resevred timeslots. 
	 * @return
	 */
	private void checkWait() 
	{
		Iterator<Integer>iterSortedSchedule = mapSchedules.keySet().iterator();
		//SimpleDateFormat sdf = new SimpleDateFormat(ConstantsDefinition.DATE_FORMAT);

		while(iterSortedSchedule.hasNext())
		{
			Integer nextScheduleDate = iterSortedSchedule.next();
			ArrayList<String>listEntries = mapSchedules.get(nextScheduleDate);

			for(int i=0; i<listEntries.size() ;i++)
			{
				String strProposedDuration =  listEntries.get(i).split(ConstantsDefinition.MSGBODY_DELIMITER)[0];
				String strLocationId = listEntries.get(i).split(ConstantsDefinition.MSGBODY_DELIMITER)[1].replace(ConstantsDefinition.LOCATION, "");
				String strReservedDuration = hmReservedPlan.get(strLocationId);
				log.debug("Agent:"+this.agentId+ " for location Id:"+ strLocationId+" reserved:"+strReservedDuration+" proposed:"+strProposedDuration);
				int iDurationComparison = DateUtil.compareTimePeriod(strReservedDuration, strProposedDuration);
				if(iDurationComparison != ConstantsDefinition.EQUAL)
				{
					//Wait introduced. proposed duration should be after the reserved duration
					try
					{
						Integer proposedStartDate = Integer.parseInt(strProposedDuration.split(ConstantsDefinition.TIMEDURATION_DELIMITER)[0]);
						Integer reservedStartDate = Integer.parseInt(strReservedDuration.split(ConstantsDefinition.TIMEDURATION_DELIMITER)[0]);

						long iDiff = proposedStartDate - reservedStartDate;

						if (iDiff < 0)
						{
							log.error("Agent:"+this.agentId+ " Proposed time" +proposedStartDate+" before reserved time " +reservedStartDate+". Shouldnt happen");
						}
						else
						{
							bWaitImposed = true;
							alWaitLocations.add(listEntries.get(i).concat(ConstantsDefinition.MSGBODY_DELIMITER).concat(String.valueOf(iDiff)));
						}


					}
					catch(Exception e)
					{
						log.debug("Agent:"+this.agentId+ " error in parsing either "+ strProposedDuration.split(ConstantsDefinition.TIMEDURATION_DELIMITER)[0]
								+" or "+strReservedDuration.split(ConstantsDefinition.TIMEDURATION_DELIMITER)[0]);
					}
				}

			}
		}//end while

	}

	/** This function will  iterate through the mapSchedules, which
	 * contains the start-time ordered list of proposals. It should
	 * check if the order is consistent in terms of the sequence of the
	 * locations proposed by the agent earlier. 
	 * 
	 */
	private boolean checkOrderingConsistency()
	{
		Iterator<Integer>iterSortedSchedule = mapSchedules.keySet().iterator();//startdate, start%end#locationId
		boolean bFirst = true;
		ArrayList<String> alLocationNames = new ArrayList<String>(); //this will maintain the order of the locations for comparison.

		while(iterSortedSchedule.hasNext())
		{
			Integer nextScheduleDate = iterSortedSchedule.next();

			//Check if there are two or more schedules for the same time. 
			//Introduces an ordering inconsistency.
			if(mapSchedules.get(nextScheduleDate).size() > 1)
			{
				return false;
			}//end if

			alLocationNames.add(mapSchedules.get(nextScheduleDate).get(0).split(ConstantsDefinition.MSGBODY_DELIMITER)[1].replace(ConstantsDefinition.LOCATION, ""));// start%end#locationId
		}

		//Get the list of locations in the ordered sequence of the startimte
		//and compare with the plan proposed by the agent in hmReservedPlan. If same return true,
		//else TODO

		String[] listReservedLocations = getOrderedReservedLocations();

		log.debug("Agent:"+this.agentId+ hmReservedPlan.keySet()+" and: "+alLocationNames);
		//verify that the sizes of the two lists are same.
		if(alLocationNames.size() == listReservedLocations.length)
		{	
			for(int iIndex = 0; iIndex < alLocationNames.size(); iIndex++)
			{

				if(!alLocationNames.get(iIndex).equalsIgnoreCase(listReservedLocations[iIndex]))
				{
					log.debug("Agent:"+this.agentId+  "not same in ordering");
					return false;
				}
			}
		}
		else
			return false;


		return true;
	}
	/**
	 *  This function will return the time-ordered sequence of reserved
	 *  location Ids as String
	 * @return Array of locationIds without L
	 */
	private String[] getOrderedReservedLocations() 
	{
		//Keep a hashmap indexed by duration
		//SimpleDateFormat sdf = new SimpleDateFormat(ConstantsDefinition.DATE_FORMAT);

		TreeMap<Integer, String> mapSchedules = new TreeMap<Integer, String>();
		String[] locationIds = new String[hmReservedPlan.keySet().size()];

		//Populate the sortedhashmap indexed with duration,
		Iterator<String> iterSchedules = hmReservedPlan.keySet().iterator();
		while(iterSchedules.hasNext())
		{
			String locationId = iterSchedules.next();
			String timeDuration = hmReservedPlan.get(locationId);
			String timeDurationSplit[] = timeDuration.split(ConstantsDefinition.TIMEDURATION_DELIMITER);


			Integer calStartTime = -1;

			try
			{ 
				calStartTime = Integer.parseInt(timeDurationSplit[0]);
			}
			catch(Exception e)
			{
				log.error("Agent:"+this.agentId+ " Cannot parse "+timeDurationSplit[0]);
			}
			//Check if mapSchedule already contains an entry for that duration
			if(mapSchedules.containsKey(calStartTime))
			{
				//Insert into the entry
				log.error("Agent:"+this.agentId+ " Two elements with same starttime");
				log.error("Making entry for loc: "+locationId+" startTime:"+calStartTime+" is conflicting with mapSchedule entry for:"+ mapSchedules.get(calStartTime)+" at same time.");
				log.error("HmReservedPlan: "+hmReservedPlan);
			}
			else
			{
				//Create an arraylist, add msg to it and add the entry, value to the map
				mapSchedules.put(calStartTime, locationId);
			}


		}//end for all msg


		//Put the sorted elements into the array
		Iterator<Integer> iterDates = mapSchedules.keySet().iterator();
		int i=0;
		while(iterDates.hasNext())
		{
			locationIds[i++] = mapSchedules.get(iterDates.next());
		}
		return locationIds;
	}

	/**
	 * This function will take a list of schedules and sort them in the increasing order of 
	 * starting time required. 
	 * @param alSchedules: Each string is of the format startTime%endTime#AgentId
	 * @return treemap:	startDate, startTime%endTime#LocationAgentId. 
	 * An arrayList of string is required as the value to a key becoz two or more locations may propose
	 * the same start date on their locations.
	 */
	private TreeMap<Integer,ArrayList<String>> sortByStartingTime(ArrayList<String> alSchedules) 
	{
		//Keep a hashmap indexed by duration
		TreeMap<Integer, ArrayList<String>> tmpMapSchedules = new TreeMap<Integer, ArrayList<String>>();

		//	SimpleDateFormat sdf = new SimpleDateFormat(ConstantsDefinition.DATE_FORMAT);

		//Populate the sortedhashmap indexed with duration, 
		for(String msg:alSchedules)
		{
			String[] msgSplit = msg.split(ConstantsDefinition.MSGBODY_DELIMITER);
			String[] timeDurationSplit = msgSplit[PERIOD_INDEX].split(ConstantsDefinition.TIMEDURATION_DELIMITER);

			try 
			{
				int calStartTime = Integer.parseInt(timeDurationSplit[0]);

				//Check if mapSchedule already contains an entry for that duration
				if(tmpMapSchedules.containsKey(calStartTime))
				{
					//Insert into the entry
					tmpMapSchedules.get(calStartTime).add(msg);
				}
				else
				{
					//Create an arraylist, add msg to it and add the entry, value to the map
					ArrayList<String> listMsg = new ArrayList<String>();
					listMsg.add(msg);
					tmpMapSchedules.put(calStartTime, listMsg);
				}
			}
			catch(Exception e)
			{
				log.error("Agent:"+this.agentId+ " Exception in parsing date "+timeDurationSplit[0]);
			}

		}//end for all msg
		return tmpMapSchedules;
	}//end function

	/**
	 *  This function will iterate through each location in the parameter
	 *  and will formulate Msg objects and then will finally transmit it to the
	 *  PO.
	 * @param locationSchedules: Key: LocationId, Value:StartDateTIMEDURATIONDELIMITERendDate
	 */
	public  ArrayList<Message>  transmitMessage()
	{
		ArrayList<Message>listMessages = new ArrayList<Message>();
		for(String locationId:hmReservedPlan.keySet())
		{
			//Compose the message
			Message msg = new Message();
			msg.setReceiver(ConstantsDefinition.LOCATION.concat(locationId));
			String senderId = ConstantsDefinition.TRAVELLER.concat(String.valueOf(this.agentId));
			switch(iProperty)
			{
			case ConstantsDefinition.LOCATION_SCHEDULE_LENGTH:
				senderId = senderId.concat(ConstantsDefinition.SENDER_DELIMITER).concat(String.valueOf(this.iLength));
				break;
			case ConstantsDefinition.LOCATION_SCHEDULE_SPEED:
				senderId = senderId.concat(ConstantsDefinition.SENDER_DELIMITER).concat(String.valueOf(this.iSpeed));
				break;
			}
			msg.setSender(senderId);
			
			msg.setBody(String.valueOf(ConstantsDefinition.RESERVE).concat(ConstantsDefinition.MSGBODY_DELIMITER)
					.concat(hmReservedPlan.get(locationId)).concat(ConstantsDefinition.MSGBODY_DELIMITER)
					.concat(String.valueOf(iDWCount)));
			log.debug("Agent:"+this.agentId+" transmit msg. sender "+msg.getSender());
			//iDWCount++;
			listMessages.add(msg);
		}
		//log.info("Agent:"+this.agentId+" transmit msg. DW = "+iDWCount);
		iDWCount++;
		return listMessages;
		//this.postOffice.receiveMessage(listMessages);
	}
	/**
	 * This function will set the source node for the agent
	 * @param string
	 */
	public void setSrcNodeId(String srcNodeId) 
	{
		strSrcNode = srcNodeId;
	}

	public String getSrcNodeId() 
	{
		return strSrcNode;
	}

	/**
	 * This function will set the source node for the agent
	 * @param string
	 */
	public void setDestNodeId(String destNodeId) 
	{
		strDestNode = destNodeId;
	}

	public String getDestNodeId() 
	{
		return strDestNode;
	}
	public void setStartDate(int startDate) 
	{
		this.startDate = startDate;
		//log.debug("Agent:"+this.agentId+" startDate"+this.startDate);
	}
	public void setSpeed(int speed)
	{
		this.iSpeed = speed;	
	}
	public void setLength(int length) {
		iLength = length;
	}
	public int getSpeed() {
		return iSpeed;
	}
	public void setEndDate(int endDate)
	{
		this.endDate = endDate;	
	}
	/*	@Override
	public void run() {

		//If it is the first run, generate the schedule and transmit the message.
		//Else keep polling to check if new msgs have been received at the postOffice 
		//and collect them.
		if(bFirstRun)
		{
			bFirstRun = false;
			generateSchedule();
		}

		else
		{
			alRxdMsg = Collections.synchronizedList(alRxdMsg);

			synchronized (alRxdMsg) 
			{
				//Check for consistency 
				checkConsistency(alRxdMsg);	
			}
		}	
	}
	 */
	/** This function will be invoked when the plan
	 * formed by the responses of the locations is consistent.
	 * Depending on the status of the agent, the response msg has
	 * to be composed and transmitted.
	 * if status is RESERVE then  ACCEPT and send ACCEPT msg
	 * if ACCEPT then DW_INIT
	 * if DW_INIT then DW_COUNT_x
	 * if DW_COUNT_x where x = DWSize, then FINALISE;
	 */
	private ArrayList<Message> transmitResponse() 
	{
		log.debug("Agent:"+this.agentId+" transmit Response>>"+agentStatus);
		//		log.info("Agent:"+this.agentId+" iDWCount:"+(iDWCount+1));

		ArrayList<Message>alResponseMsg = new ArrayList<Message>();
		if(agentStatus == ConstantsDefinition.RESERVE)
		{
			if(iDWCount + 1 <= iDWSize)
			{
				agentStatus = ConstantsDefinition.ACCEPT;
				String senderId = ConstantsDefinition.TRAVELLER.concat(String.valueOf(this.agentId));
				switch(iProperty)
				{
				case ConstantsDefinition.LOCATION_SCHEDULE_LENGTH:
					senderId = senderId.concat(ConstantsDefinition.SENDER_DELIMITER).concat(String.valueOf(this.iLength));
					break;
				case ConstantsDefinition.LOCATION_SCHEDULE_SPEED:
					senderId = senderId.concat(ConstantsDefinition.SENDER_DELIMITER).concat(String.valueOf(this.iSpeed));
					break;
				}
				for(Message msg:alRxdMsg)
				{
					log.debug("Agent:"+this.agentId+" Rxd msg::"+msg);
					Message respMsg = new Message();
					respMsg.setReceiver(msg.getSender());
					respMsg.setSender(senderId);
					String msgBody[] = msg.getBody().split(ConstantsDefinition.MSGBODY_DELIMITER);
					String respBody = ConstantsDefinition.ACCEPT.toString().concat(ConstantsDefinition.MSGBODY_DELIMITER)
							.concat(msgBody[1]).concat(ConstantsDefinition.MSGBODY_DELIMITER)
							.concat(String.valueOf(iDWCount));
					log.debug("Agent:"+this.agentId+" response msg. sender "+respMsg.getSender());
					
					respMsg.setBody(respBody);	
					alResponseMsg.add(respMsg);
				}
				log.debug("Agent:"+this.agentId+" agentStatus"+agentStatus);
				long after =Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory(); 

				//System.out.println("memory: "+(after-before));
				iDWCount++;
				return sendMessage(alResponseMsg);
			}
			else
			{
				agentStatus = ConstantsDefinition.TERMINATE;
				log.debug("Agent:"+this.agentId+" agentStatus"+agentStatus+" iDWCount:"+iDWCount);
				return null;
			}
		}
		else if(agentStatus == ConstantsDefinition.ACCEPT)
		{
			//If all agents have sent an accept, then initialise a DW or else
			//there is atleast one agent which has sent a propose, which has been 
			//found consistent, so send an accept again.
			String senderId = ConstantsDefinition.TRAVELLER.concat(String.valueOf(this.agentId));
			switch(iProperty)
			{
			case ConstantsDefinition.LOCATION_SCHEDULE_LENGTH:
				senderId = senderId.concat(ConstantsDefinition.SENDER_DELIMITER).concat(String.valueOf(this.iLength));
				break;
			case ConstantsDefinition.LOCATION_SCHEDULE_SPEED:
				senderId = senderId.concat(ConstantsDefinition.SENDER_DELIMITER).concat(String.valueOf(this.iSpeed));
				break;
			}
			for(Message msg:alRxdMsg)
			{
				Message respMsg = new Message();
				respMsg.setReceiver(msg.getSender());
				respMsg.setSender(senderId);
				String msgBody[] = msg.getBody().split(ConstantsDefinition.MSGBODY_DELIMITER);
				String responseStatus= bAcceptRxdFromAllLocations?ConstantsDefinition.DW.toString():ConstantsDefinition.ACCEPT.toString();
				String dwStatus = bAcceptRxdFromAllLocations?String.valueOf(ConstantsDefinition.DWINITCOUNT):String.valueOf(ConstantsDefinition.DW_UNINIT);
				/*
				 * String respBody =
				 * responseStatus.concat(ConstantsDefinition.MSGBODY_DELIMITER).concat(msgBody[1
				 * ]) .concat(ConstantsDefinition.MSGBODY_DELIMITER).concat(dwStatus);
				 */
				String respBody = responseStatus.concat(ConstantsDefinition.MSGBODY_DELIMITER).concat(msgBody[1])
						.concat(ConstantsDefinition.MSGBODY_DELIMITER).concat(String.valueOf(iDWCount));

				respMsg.setBody(respBody);	
				log.debug("Agent:"+this.agentId+" response msg. sender "+respMsg.getSender());
				
				alResponseMsg.add(respMsg);
			}

			//If accept has been received from all agents, change status to DW
			if(bAcceptRxdFromAllLocations)
			{
				if (iDWCount +1 <= iDWSize)
					agentStatus = ConstantsDefinition.DW;
				else
					agentStatus = ConstantsDefinition.FINALIZE;
				bAcceptRxdFromAllLocations = false;

			}
			if (iDWCount +1 <= iDWSize)
			{
				iDWCount++;
				return sendMessage(alResponseMsg);
			}
			else
			{
				agentStatus = agentStatus==ConstantsDefinition.DW?ConstantsDefinition.TERMINATE:ConstantsDefinition.FINALIZE;
				log.debug("Agent:"+this.agentId+" agentStatus"+agentStatus+" iDWCount:"+iDWCount);
				int calLatest = INVALID;
				String locations="";
				for(int i=0; i<alRxdMsg.size(); i++)
				{
					locations = locations.concat(alRxdMsg.get(i).getSender().replace(ConstantsDefinition.LOCATION, "")).concat(EQUALS);

					try
					{
						String timeDuration = alRxdMsg.get(i).getBody().split(ConstantsDefinition.MSGBODY_DELIMITER)[1];
						locations = locations.concat(timeDuration).concat(HASH);

						int calCurrentEnd = Integer.parseInt(timeDuration.split(ConstantsDefinition.TIMEDURATION_DELIMITER)[1]);
						if(i==0) 
						{
							calLatest = Integer.parseInt(timeDuration.split(ConstantsDefinition.TIMEDURATION_DELIMITER)[1]);
						}
						else
						{
							if(calCurrentEnd > calLatest)
								calLatest = calCurrentEnd; 
						}
					}catch(Exception e)
					{
						e.printStackTrace();
					}
				}
				agentCost = calLatest-this.startDate;
				log.debug("Finalised[ Agent Id:"+agentId+" Agent Cost:" + agentCost+" Final schedule:"+locations+ " init cost:"+lInitialCost+" init schedule:"+strInitialSchedule);
				log.debug("Agent Id:"+agentId+" DWcount:"+iDWCount+" Return null");
				return null;
			}
		}//end if Accept received from all agents
		else if(agentStatus == ConstantsDefinition.DW) 
		{ 
			String senderId = ConstantsDefinition.TRAVELLER.concat(String.valueOf(this.agentId));
			switch(iProperty)
			{
			case ConstantsDefinition.LOCATION_SCHEDULE_LENGTH:
				senderId = senderId.concat(ConstantsDefinition.SENDER_DELIMITER).concat(String.valueOf(this.iLength));
				break;
			case ConstantsDefinition.LOCATION_SCHEDULE_SPEED:
				senderId = senderId.concat(ConstantsDefinition.SENDER_DELIMITER).concat(String.valueOf(this.iSpeed));
				break;
			}
			for(Message msg:alRxdMsg)
			{
				Message respMsg = new Message();
				respMsg.setReceiver(msg.getSender());
				respMsg.setSender(senderId);
				log.debug("Agent:"+this.agentId+" transmit msg. sender "+respMsg.getSender());
				
				String msgBody[] = msg.getBody().split(ConstantsDefinition.MSGBODY_DELIMITER);
				log.debug(msg.getBody());
				//Check if the DW window has reached its DWSize, if so send finalise, else increment the count.
				Integer msgStatus;
				Integer dwCount;
				if(iDWCount+1 <= iDWSize)
				{
					msgStatus = ConstantsDefinition.DW;
					dwCount = iDWCount + 1;
				}
				else
				{
					msgStatus = ConstantsDefinition.FINALIZE;
					dwCount = ConstantsDefinition.DW_UNINIT;
					agentStatus = ConstantsDefinition.FINALIZE;
					bFinalisedMessageRxd = true;
				}

				/*
				 * String respBody =
				 * msgStatus.toString().concat(ConstantsDefinition.MSGBODY_DELIMITER)
				 * .concat(msgBody[1]).concat(ConstantsDefinition.MSGBODY_DELIMITER)
				 * .concat(String.valueOf(dwCount));
				 */
				String respBody = msgStatus.toString().concat(ConstantsDefinition.MSGBODY_DELIMITER)
						.concat(msgBody[1]).concat(ConstantsDefinition.MSGBODY_DELIMITER)
						.concat(String.valueOf(iDWCount));
				respMsg.setBody(respBody);	
				alResponseMsg.add(respMsg);
			}
			log.debug("Agent:"+this.agentId+" agentStatus"+agentStatus+" iDWCount:"+iDWCount);
			iDWCount++;

			if(agentStatus == ConstantsDefinition.FINALIZE) 
			{

				String locations="";
				int calLatest = INVALID;
				String strFinalisedSchedule="";
				for(int i=0; i<alRxdMsg.size(); i++)
				{
					locations = locations.concat(alRxdMsg.get(i).getSender().replace(ConstantsDefinition.LOCATION, "")).concat(EQUALS);

					try
					{
						String timeDuration = alRxdMsg.get(i).getBody().split(ConstantsDefinition.MSGBODY_DELIMITER)[1];
						locations = locations.concat(timeDuration).concat(HASH);
						int calCurrentEnd = Integer.parseInt(timeDuration.split(ConstantsDefinition.TIMEDURATION_DELIMITER)[1]);

						if(i==0) {

							calLatest = Integer.parseInt(timeDuration.split(ConstantsDefinition.TIMEDURATION_DELIMITER)[1]);
						} else {
							if(calCurrentEnd > calLatest)
								calLatest = calCurrentEnd; 
						}
					}catch(Exception e)
					{
						e.printStackTrace();
					}
				}
				agentCost = calLatest-this.startDate;
				log.debug("Finalised[ Agent Id:"+agentId+" Agent Cost:" + agentCost+" Final schedule:"+locations+ " init cost:"+lInitialCost+" init schedule:"+strInitialSchedule);
				log.debug("Agent Id:"+agentId+" DWcount:"+iDWCount+" Return null");
				//Send a completion message to the postOffice
				/*ACLMessage message = new ACLMessage(ACLMessage.INFORM);
			message.setContent("Done"+RATE+locations+RATE+agentCost);
			message.addReceiver(new AID(ConstantsDefinition.PO_AGENT, AID.ISLOCALNAME));
			myAgent.send(message);
				 */
				return null;
			}
			return sendMessage(alResponseMsg);
		}
		//check the dw size and the current count, and then finalise. 
		log.debug("Agent Id:"+agentId+" DWcount:"+iDWCount+" Return null");

		return null;
	}

	private ArrayList<Message> sendMessage(ArrayList<Message> listMessages) 
	{
		iAgentsMessagedTo = 0;
		for(Message msg:listMessages)
		{
			if(!msg.getBody().split(ConstantsDefinition.MSGBODY_DELIMITER)[0].equalsIgnoreCase(ConstantsDefinition.REJECT.toString()))
				iAgentsMessagedTo++;
		}
		log.debug("Agent Id:"+agentId+" DWcount:"+iDWCount +" Return "+listMessages.size());

		return listMessages;

	}

	/**
	 * This function will divide the received list of responses
	 * from all the relevant edges based on the type of msg. 
	 * It will populate the hmProposedPlan based on the rxd msgs
	 * @param alRxdMsg: list of all msgs received from location agents.
	 */
	private void divideBasedOnMsgType()
	{
		ArrayList<String> alAcceptedPlan = new ArrayList<String>();  //maintains the list of accepted locations.
		hmProposedPlan.clear(); //edgeId, timeDuration
		for(Message msg:alRxdMsg)
		{
			log.debug(msg.toString());
			String msgBody = msg.getBody();
			String msgBodyContents[] = msgBody.split(ConstantsDefinition.MSGBODY_DELIMITER);
			Integer msgStatus = Integer.parseInt(msgBodyContents[0]); 
			if(msgStatus == ConstantsDefinition.PROPOSE)
			{
				hmProposedPlan.put(msg.getSender(), msgBodyContents[1]);
			}
			else if (msgStatus == ConstantsDefinition.ACCEPT)
			{
				//Add the accepted schedule to the list of the proposedPlan
				//and then add the locationId to the list of AcceptedLocation.
				hmProposedPlan.put(msg.getSender(),msgBodyContents[1]);
				alAcceptedPlan.add(msg.getSender());
			}
			else if(msgStatus == ConstantsDefinition.DW)
			{
				/*
				 * Integer iDWCount =
				 * Integer.parseInt(msgBodyContents[ConstantsDefinition.DWCOUNT_INDEX]);
				 * hmDW.put(msg.getSender(), iDWCount);
				 */
				hmProposedPlan.put(msg.getSender(),msgBodyContents[1]);
				log.debug("Agent:"+this.agentId+" DW received. Doing nothing in divide based on msg type");
			}
			else if(msgStatus == ConstantsDefinition.FINALIZE)
			{
				log.debug("Agent:"+this.agentId+" FINALISE received. Should not happen");
			}
			else
			{
				log.error("Rxd msg from"+msg.getSender()+" with an undefined status as "+msgStatus);
			}
		}//end for 

		log.debug("Agent:"+this.agentId+" alAcceptedPlan.size()"+alAcceptedPlan.size()+" iAgentsMessagedTo"+iAgentsMessagedTo);
		//Check whether accpet has been recieved from all agents
		if(alAcceptedPlan.size() == iAgentsMessagedTo)
			bAcceptRxdFromAllLocations = true;
	}//end function

	public int getAgentStatus() {
		return agentStatus;
	}

	public long getInitialCost() 
	{
		return lInitialCost;
	}


}