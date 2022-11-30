package dmapf.agents;
/**
 *  This class will be the edge of the road network. It will receive
 *  reservations for time-schedules on itself. Based on the reservations
 *  received, it will generate an optimal schedule for all requests and 
 *  send them as response. 
 *  After the Location agent sends its response, it may receive an acceptance
 *  from a Traveller agent or may receive reservation from other agents.
 *  Depending upon the deliberation window, it may revise an earlier acceptance,to
 *  generate an optimal schedule based on newly received requests. If the deliberation window
 *  is closed then it will mark an accepted response as reserved and will not allocate
 *  it or change it further.
 *  The Location will have to maintain a status wrt every Traveller it interacts with. 
 *  Once a Location receives a reserve request from a Traveller,thereafter it will continuea
 *  to get some msgs from the Traveller, till either the dw is elapsed and the confirmed, or 
 *  the Traveller rejects the request. If the Traveller rejects the request, then its status is removed
 *  from the Location's records. If a travellers request is confirmed, ie the schedule is 
 *  maintained for its dw, then then location will mark the schedule in its confirmed list.
 *  Thereafter it should not receive any more msgs from that Traveller, if it receives then it 
 *  is an error. 
 *  
 * @author st
 *
 */

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.List;
import java.util.Map;

import javax.print.attribute.HashAttributeSet;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import dmapf.constants.ConstantsDefinition;
import dmapf.model.Constraint;
import dmapf.model.Message;
import dmapf.utils.DateUtil;
//TODO Handle the DW message
public class LocationAgent {

	private int iLocationId;
	private int iDWSize; //duration of the deliberation window. Setup at initialisation.
	private PostOffice postOffice;
	private static final int TPP = 4;
	private static final int GAP = 2;
	int iOverLapIndex; //maintains the first index with the proposed schedule where overlap takes place
	int iOverlapConstraintIndex; //maintains the first index with the constraints where overlap takes place
	private static final int INVALID = -1;


	private ArrayList<Integer> alAgents = new ArrayList<Integer>(); //List of known agentIds  
	private ArrayList<Integer> alExpectedStatus = new ArrayList<Integer>(); //index of alExpectedStatus is same as alAgents.
	private ArrayList<String> alFinalisedAgents = new ArrayList<String>(); //list of agents which are finalised. should not have anything is common with alAgents.

	private ArrayList<String> alReservations = new ArrayList<String>(); //list of 	reservations received in current tic 
	private ArrayList<String> alRejections = new	ArrayList<String>(); //list of rejections reveived in current tic private
	private ArrayList<String> alAcceptance = new ArrayList<String>(); //list of acceptances received in current tic
	private ArrayList<String> alFinalise = new ArrayList<String>(); //list of finalises received in current tic
	private ArrayList<String> alDWs = new ArrayList<String>(); //list of DWs received in current tic
	ArrayList<String> alProposedSchedule = new ArrayList<String>();//list of proposed Schedule

	private ArrayList<Constraint> alConstraints = new ArrayList<Constraint>(); //list of confirmed schedules.
	private List<Message> alRxdMsg = new ArrayList<Message>();//list of rxd msgs from all agents
	/*
	 * private ArrayList<> alSchedule = new ArrayList<E>(); //list of generated
	 * Schedule based on current statuses
	 */
	private ArrayList<Integer> alDeliberationWindow = new ArrayList<Integer>(); //list maintaining DW elapsed.
	private boolean bRescheduled; //this flag will maintain if rescheduling of the plans has taken place because of rxf reserves or rejects.

	private int iAllocStrategy; //maintains the allocation strategy
	public static final Logger log = Logger.getLogger(LocationAgent.class);
	private HashMap<String, Integer> hmTravellerProperty = new HashMap<String, Integer>();
	/**
	 * Constructor
	 * @param iAgentId
	 * @param dwSize: duration of deliberation window.
	 */
	public LocationAgent(int iAgentId, int dwSize, PostOffice poAgent, int iAllocStrat)
	{
		iLocationId = iAgentId;
		iDWSize = dwSize;
		this.postOffice = poAgent;
		PropertyConfigurator.configure("resources/config-properties/log4j.properties");
		this.iAllocStrategy = iAllocStrat;


	}

	/**
	 *  This function will receive the message from the PO. It will iterate through
	 *  all the messages and validate each of them. Consequent to validation, it 
	 *  will regenerate the schedule depending on new reserves/rejects. Depending on the 
	 *  schedule, it will formulate new message to be sent to the resp agents.
	 *  It will also update constraints if the DW is elapsed. 
	 *   
	 *  @param alRxdMsg: list of all messages to this location. There should only be one message from any traveller.
	 */
	public void receiveMessage(ArrayList<Message> alRxdMsgFromPO)
	{
		log.debug("L"+this.getLocationId()+" receive Msg");
		//	alRxdMsg =  Collections.synchronizedList(alRxdMsg);

		//synchronized (alRxdMsg) 
		{
			alRxdMsg.clear();
			alRxdMsg.addAll(alRxdMsgFromPO);

			//Check if the msg is sent by a known agent or new agent
			//if (validateMsg(alRxdMsg))
			//return processMsg(alRxdMsg);
			//else
			//return null;
			//alRxdMsg.addAll(alRxdMsgFromPO);
			//log.debug("Location"+iLocationId+": Msg List:"+alRxdMsg);

		}
	}


	/**
	 * This function will receive the message and depending on 
	 * the status of the msg, will update the corresponding list.
	 * Once the data is updated in the corresponding lists, rescheduling
	 * of the plans may happen. First dw has to be checked and list
	 * of constraints will have to be updated accordingly.
	 *  
	 * @param msg: valid Message List from traveller to be processed.
	 */
	public ArrayList<Message> processMsg(/*List<Message> alRxdMsg*/) 
	{
		//		log.debug("process Msg");
		//if (validateMsg(alRxdMsg))
		{
			alReservations.clear();
			//	alAcceptance.clear();
			alRejections.clear();
			//	alDWs.clear();
			//	alFinalise.clear();
			//Put all msgs into their corresponding bins.
			for(Message msg:alRxdMsg)
			{
				//Get the status of the msg and update the corresponding list
				Integer status = Integer.parseInt(msg.getBody().split(ConstantsDefinition.MSGBODY_DELIMITER)[ConstantsDefinition.STATUS_INDEX]);
				String[] splitMsg = msg.getBody().split(ConstantsDefinition.MSGBODY_DELIMITER);
				String senderId = msg.getSender().split(ConstantsDefinition.SENDER_DELIMITER)[0];
				String content = splitMsg[ConstantsDefinition.STATUS_INDEX]
						.concat(ConstantsDefinition.MSGBODY_DELIMITER)
						.concat(splitMsg[ConstantsDefinition.PERIOD_INDEX])
						.concat(ConstantsDefinition.MSGBODY_DELIMITER)
						.concat(senderId)
						.concat(ConstantsDefinition.MSGBODY_DELIMITER)
						.concat(splitMsg[2]);
				log.debug("L"+this.iLocationId+ " rxd msg from sender"+msg.getSender());

				if(iAllocStrategy == ConstantsDefinition.LOCATION_SCHEDULE_LENGTH || iAllocStrategy == ConstantsDefinition.LOCATION_SCHEDULE_SPEED)
					hmTravellerProperty.put(senderId,Integer.parseInt(msg.getSender().split(ConstantsDefinition.SENDER_DELIMITER)[1]));

				log.debug("L"+this.iLocationId+ " rxd msg body"+msg.getBody()+" generated content "+content);
				if(status!=ConstantsDefinition.REJECT)
					alReservations.add(content);
				else 
					alRejections.add(content);

			}

			//Update the list of requests to be scheduled.
			return updateScheduleList();
		}
		//	return null;	

	}//end processMsg

	/**
	 * This function will take in the list of rejections and delete the entries
	 * from the agent and status tables.
	 * The reserves, accepts, dw will be taken and if reserves > 0, rescheduled. 
	 * Before rescheduling, what has to be done about the dw.. 
	 */
	private ArrayList<Message> updateScheduleList() 
	{
		//remove the rejections from the list.
		//removeRejects();

		//move the finalises to the constraints.
		//updateConstraints();

		//validate the dws. 
		//validateDW();

		//schedule on the reserves, accepts and transmit the responses.
		return scheduleAndTransmit();


	}

	/**
	 *  This function will send msgs to the PO. The msgs sent by the LocationAgent
	 *  will contain the following fields as status: PROPOSE,DW_INIT, NOCHANGE, COMPLETE.
	 *  
	 *  To send the msgs, it will first iterate through the received msgs in alSchedules, 
	 *  to compute which status msg should be inserted. Then it will add the revised or
	 *  same schedule and then set the sender and receiver fields and transmit to the PO.
	 */
	private ArrayList<Message> transmit(ArrayList<String>alSchedules) 
	{
		ArrayList<Message>alMsgToTransmit = new ArrayList<>();
		if(bRescheduled)
		{
			for(String msg:alSchedules)
			{
				alMsgToTransmit.add(composeResponse(msg));
			}
			bRescheduled = false;
		}
		else
		{
			log.error("L"+iLocationId+" Shouldnt come here");
			//there has been no rescheduling. so iterate only thru the received messages and 
			//generate response accordingyl.
			/*for(String msg:alSchedules)
			{
				alMsgToTransmit.add(redraftResponse(msg));
			}*/
		}
		if(alFinalise.size() > 0)
		{
			log.error("L"+this.iLocationId+" Shouldnt come here");
			/*
			 * for(String strMsg:alFinalise) { Message msg = new Message(); String
			 * strMsgSplit[] = strMsg.split(ConstantsDefinition.MSGBODY_DELIMITER);
			 * 
			 * msg.setSender(ConstantsDefinition.LOCATION.concat(String.valueOf(this.
			 * iLocationId)));
			 * msg.setReceiver(strMsgSplit[ConstantsDefinition.AGENTID_INDEX]);
			 * 
			 * String msgBody = ""; //Each strMsg ois of the form timePeriod#AgentId
			 * 
			 * //get the proposed time period msgBody =
			 * strMsg.substring(0,strMsg.lastIndexOf(ConstantsDefinition.MSGBODY_DELIMITER))
			 * ; //eliminate the agent id from the string msg.setBody(msgBody);
			 * log.debug("L"+iLocationId+" composed:"+msg.getBody()+" for "+msg.getReceiver(
			 * )); alMsgToTransmit.add(msg);
			 * 
			 * }//end for finalise
			 */		}
		return alMsgToTransmit;
		//postOffice.receiveMessage(alMsgToTransmit);
	}
	/**
	 *  This function will be invoked when there has been no rescheduling of
	 *  the plans in the current iteration. In this case, the msgs will be composed
	 *  as-is  based on the received msg, only the status may get changed. 
	 *  and the receiver and senders are updated accordingly. 
	 * @param msg status#timePeriod#AgentId
	 * @return
	 */
	private Message redraftResponse(String strRxdMsg) 
	{
		log.debug("L"+iLocationId+" redraftResponse"+strRxdMsg+" Shouldnt happen.******");
		Message msg = new Message();
		String rxdMsgSplit[] = strRxdMsg.split(ConstantsDefinition.MSGBODY_DELIMITER);

		msg.setSender(ConstantsDefinition.LOCATION.concat(String.valueOf(this.iLocationId)));
		msg.setReceiver(rxdMsgSplit[ConstantsDefinition.AGENTID_INDEX]);

		String msgBody = "";
		//Each strMsg ois of the form timePeriod#AgentId

		//get the proposed time period
		if(Integer.parseInt(rxdMsgSplit[ConstantsDefinition.STATUS_INDEX]) == ConstantsDefinition.RESERVE)
		{
			log.error("RESERVE msg. Should not have come here.");
		}
		else if(Integer.parseInt(rxdMsgSplit[ConstantsDefinition.STATUS_INDEX]) == ConstantsDefinition.ACCEPT
				|| Integer.parseInt(rxdMsgSplit[ConstantsDefinition.STATUS_INDEX]) == ConstantsDefinition.DW
				|| Integer.parseInt(rxdMsgSplit[ConstantsDefinition.STATUS_INDEX]) == ConstantsDefinition.FINALIZE)
		{
			//If ACCEPT msg was rxd, and no change then resend the same msgbody back.
			//msgBody = strRxdMsg.substring(0,strRxdMsg.lastIndexOf(ConstantsDefinition.MSGBODY_DELIMITER)); //eliminate the agent id from the string
			msgBody = rxdMsgSplit[ConstantsDefinition.STATUS_INDEX]
					.concat(ConstantsDefinition.MSGBODY_DELIMITER)
					.concat(rxdMsgSplit[ConstantsDefinition.PERIOD_INDEX])
					.concat(ConstantsDefinition.MSGBODY_DELIMITER)
					.concat(rxdMsgSplit[ConstantsDefinition.DWCOUNT_INDEX]);

			msg.setBody(msgBody);
			log.debug("L"+iLocationId+" composed:"+msg.getBody()+" for "+msg.getReceiver());
			return msg;
		}
		else
		{
			log.debug("Rxd msg with status "+Integer.parseInt(rxdMsgSplit[ConstantsDefinition.STATUS_INDEX])+" which is not handled.");
		}
		log.debug("Returning "+msg);
		return msg;
	}
	/**
	 *  This function will look into the rxdstatus msg and compose the response msg accordingly.
	 * @param strRxdMsg : Received msgbody in the form status#timePeriod#AgentId
	 * 
	 * @return Message msg with body in the format statusMSGBODYDELIMITERTimePeriod
	 */
	private Message composeResponse(String strRxdMsg)
	{
		log.debug("compose Response:>>"+strRxdMsg+" alProposedSchedule"+alProposedSchedule);
		Message msg = new Message();
		String rxdMsgSplit[] = strRxdMsg.split(ConstantsDefinition.MSGBODY_DELIMITER);

		msg.setSender(ConstantsDefinition.LOCATION.concat(String.valueOf(this.iLocationId)));
		msg.setReceiver(rxdMsgSplit[ConstantsDefinition.AGENTID_INDEX]);

		String msgBody = "";
		//Iterate thru the proposed schedule to find out what should be sent as msg body
		//Each strMsg ois of the form timePeriod#AgentId
		for(String strMsg:alProposedSchedule)
		{
			log.debug("L"+iLocationId+" strMsg: "+strMsg);

			//Check whether the proposed schedule is for the current agent
			if(strMsg.endsWith(rxdMsgSplit[ConstantsDefinition.AGENTID_INDEX]))
			{
				log.debug("L"+iLocationId+" "+strMsg +" contains "+rxdMsgSplit[ConstantsDefinition.AGENTID_INDEX]);

				//get the proposed time period
				String msgSplit[] = strMsg.split(ConstantsDefinition.MSGBODY_DELIMITER);
				if(Integer.parseInt(rxdMsgSplit[ConstantsDefinition.STATUS_INDEX]) == ConstantsDefinition.RESERVE)
				{
					msgBody = String.valueOf(ConstantsDefinition.PROPOSE)
							.concat(ConstantsDefinition.MSGBODY_DELIMITER).concat(msgSplit[1])
							.concat(ConstantsDefinition.MSGBODY_DELIMITER).concat(rxdMsgSplit[ConstantsDefinition.DWCOUNT_INDEX]);
					msg.setBody(msgBody);
					log.debug("L"+iLocationId+" composed:"+msg.getBody()+" for "+msg.getReceiver());
					return msg;
				}
				else if(Integer.parseInt(rxdMsgSplit[ConstantsDefinition.STATUS_INDEX]) == ConstantsDefinition.ACCEPT)
				{
					//Check if the timePeriod accepted by the Traveller is the same as the timePeriod reproposed by
					//the location. If yes, send accept, if no, then send propose with new timeperiod
					String rxdTimePeriod = rxdMsgSplit[1];
					if(rxdTimePeriod.equalsIgnoreCase(msgSplit[1]))
						msgBody = String.valueOf(ConstantsDefinition.ACCEPT).concat(ConstantsDefinition.MSGBODY_DELIMITER)
						.concat(msgSplit[1]).concat(ConstantsDefinition.MSGBODY_DELIMITER)
						.concat(rxdMsgSplit[ConstantsDefinition.DWCOUNT_INDEX]);
					else
						msgBody = String.valueOf(ConstantsDefinition.PROPOSE).concat(ConstantsDefinition.MSGBODY_DELIMITER)
						.concat(msgSplit[1]).concat(ConstantsDefinition.MSGBODY_DELIMITER)
						.concat(rxdMsgSplit[ConstantsDefinition.DWCOUNT_INDEX]);

					msg.setBody(msgBody);
					log.debug("L"+iLocationId+" composed:"+msg.getBody()+" for "+msg.getReceiver());
					return msg;
				}
				else if(Integer.parseInt(rxdMsgSplit[ConstantsDefinition.STATUS_INDEX]) == ConstantsDefinition.DW)
				{
					//Check whether the proposed solution is same as the earlier commited version, if so 
					//increment the DW count, else reset to PROPOSE
					if(msgSplit[1].equalsIgnoreCase(rxdMsgSplit[1]))
					{

						msgBody = String.valueOf(ConstantsDefinition.DW)
								.concat(ConstantsDefinition.MSGBODY_DELIMITER).concat(msgSplit[1])
								.concat(ConstantsDefinition.MSGBODY_DELIMITER).concat(rxdMsgSplit[ConstantsDefinition.DWCOUNT_INDEX]);
						msg.setBody(msgBody);
					}
					else
					{

						msgBody = String.valueOf(ConstantsDefinition.PROPOSE)
								.concat(ConstantsDefinition.MSGBODY_DELIMITER).concat(msgSplit[1])
								.concat(ConstantsDefinition.MSGBODY_DELIMITER).concat(rxdMsgSplit[ConstantsDefinition.DWCOUNT_INDEX]);
						msg.setBody(msgBody);
					}
					log.debug("L"+iLocationId+" composed:"+msg.getBody()+" for "+msg.getReceiver());
					return msg;
				}
			}
		}

		log.debug("Msg composed"+msg);
		return msg;
	}

	/** 
	 *  This method will validate the DW msgs received. Ideally the Traveller
	 *  has to keep a check on the DW and send the count along with each DW msg. 
	 *  At no time, the DW count should go more that DWsize. This function will
	 *  only verify that the msgs received are so.
	 */
	private void validateDW() 
	{

		log.debug("L"+this.iLocationId+" validateDW"+alDWs.size());

		for(String strDW:alDWs)
		{
			int iAgentId = Integer.parseInt(strDW.split(ConstantsDefinition.MSGBODY_DELIMITER)[ConstantsDefinition.AGENTID_INDEX].replace(ConstantsDefinition.TRAVELLER, ""));
			int iDWCount = Integer.parseInt(strDW.split(ConstantsDefinition.MSGBODY_DELIMITER)[ConstantsDefinition.DWCOUNT_INDEX]);
			log.debug("L"+this.iLocationId+" DWCount "+iDWCount);
			//The agentID should be in the list of DWs and the count should be < DWsize
			if(!alDeliberationWindow.contains(iAgentId))
			{
				//This is the first message and hence count should be 1 and added to the list
				if(iDWCount == ConstantsDefinition.DWINITCOUNT)
				{
					alDeliberationWindow.add(iAgentId);
				}
				else
				{
					//log.error("DW not in the list, and count >1" +strDW);
				}
			}
			else
			{
				//The dw init msg has been received from an agent who had already initialised
				if(iDWCount == ConstantsDefinition.DWINITCOUNT)
				{
					log.error("DW init msg received from "+iAgentId+" again.");
				}
				else if(iDWCount >= iDWSize)
				{
					log.error("DW Count has exceeded the DW limit.");
				}

			}
		}
	}

	/**
	 * This function will take the list of all the reserves and accepts and will
	 * reschedule.
	 */
	private ArrayList<Message> scheduleAndTransmit() 
	{
		ArrayList<String> alSchedules = new ArrayList<String>(); //tmp list to hold elements to be scheduled

		//Add the reserves to the alSchedules
		alSchedules.addAll(alReservations);

		//Add the accepts to the alSchedules
		//alSchedules.addAll(alAcceptance);

		//Add the dws to the alSchedules
		//alSchedules.addAll(alDWs);

		//Regenerate the schedule only if new reservations have been received or new rejections
		//have occured, else the old schedule is fine. //TODO Verify that it is so.
		if(alReservations.size() > 0 )
		{
			//generateSchedule(alSchedules, ConstantsDefinition.LOCATION_SCHEDULE_SPT );
			//generateSchedule(alSchedules, ConstantsDefinition.LOCATION_SCHEDULE_LENGTH );
			//generateSchedule(alSchedules, ConstantsDefinition.LOCATION_SCHEDULE_SPEED );
			generateSchedule(alSchedules);	
			bRescheduled = true;
		}

		return transmit(alSchedules);
	}

	/**
	 *  This function gets the list of activities which has to be scheduled.
	 *  This list only contains reserves, accepts, dws. All rejects have been 
	 *  handles earlier, finalises moved to constraints. So in this function,
	 *  based on available constraints, it should do the scheduling to optimise maximum
	 *  satisfaction.
	 *  @param alSchedules: Each string is of the format status#startTime%endTime#AgentId#DWcount
	 *  @param iScheduleStrategy: either  LOCATION_SCHEDULE_OPTIMISE or LOCATION_SCHEDULE_SATISFYMAX
	 */
	private void generateSchedule(ArrayList<String> alSchedules)
	{

		log.debug("L"+this.iLocationId+" generateSchedule"+alSchedules+" iAllocStrateggy:"+iAllocStrategy);
		alProposedSchedule.clear();
		if(iAllocStrategy == ConstantsDefinition.LOCATION_SCHEDULE_SPT)
			alProposedSchedule.addAll(sptSchedule(alSchedules));
		else if(iAllocStrategy == ConstantsDefinition.LOCATION_SCHEDULE_SATISFYMAX)
			alProposedSchedule.addAll(maximiseSatisfy(alSchedules));
		else if(iAllocStrategy == ConstantsDefinition.LOCATION_SCHEDULE_SATISFY_PRTY)
			alProposedSchedule.addAll(prtySatisfy(alSchedules));
		else if(iAllocStrategy == ConstantsDefinition.LOCATION_SCHEDULE_SPEED)
			alProposedSchedule.addAll(speedSatisfy(alSchedules));
		else if(iAllocStrategy == ConstantsDefinition.LOCATION_SCHEDULE_LENGTH)
			alProposedSchedule.addAll(lengthSatisfy(alSchedules));

		else
			log.error("Undefined schedule strategy for location");

	}
	private ArrayList<String> prtySatisfy(ArrayList<String> alSchedules) 
	{
		//Sort all the schedules by the shortest processing time. 
		//Beginning with the first, keep allocating each request till it overlaps with
		//either a constraint or previous allocation. If the previous allocation had the same starting 
		//time, then shift the conflicting allocation after completion of the previous one.  
		TreeMap<Integer, ArrayList<String>>sortedSchedules = new TreeMap<Integer, ArrayList<String>>();
		sortedSchedules.putAll(sortByPrty(alSchedules));
		return generateSortedSchedule(sortedSchedules);
	}





	private ArrayList<String> lengthSatisfy(ArrayList<String> alSchedules) 
	{
		//Sort all the schedules by the shortest processing time. 
		//Beginning with the first, keep allocating each request till it overlaps with
		//either a constraint or previous allocation. If the previous allocation had the same starting 
		//time, then shift the conflicting allocation after completion of the previous one.
		log.debug("Schedule by Length");
		TreeMap<Integer, ArrayList<String>>sortedSchedules = new TreeMap<Integer, ArrayList<String>>();
		sortedSchedules.putAll(sortByLength(alSchedules));

		return generateSortedSchedule(sortedSchedules);
	}




	private ArrayList<String> speedSatisfy(ArrayList<String> alSchedules) 
	{
		//Sort all the schedules by their speed. 
		//Beginning with the first, keep allocating each request till it overlaps with
		//either a constraint or previous allocation. If the previous allocation had the same starting 
		//time, then shift the conflicting allocation after completion of the previous one.  
		TreeMap<Integer, ArrayList<String>>sortedSchedules = new TreeMap<Integer, ArrayList<String>>();
		sortedSchedules.putAll(sortBySpeed(alSchedules));
		return generateSortedSchedule(sortedSchedules);
	}

	/**
	 * 
	 *  The agent prty is implicitly assumed in its agent id. ie agent id 1 > agent id 2
	 * @param alSchedules : list of schedule comprising  0#2017-04-24:1700%2017-04-24:1806#-1#T1 status#timePeriod#DW#AgentId
	 * @return
	 */

	private TreeMap<Integer, ArrayList<String>> sortByPrty(ArrayList<String> alSchedules)
	{
		//Keep a hashmap indexed by agent prty
		TreeMap<Integer, ArrayList<String>> mapSchedules = new TreeMap<Integer, ArrayList<String>>();


		//Populate the sortedhashmap indexed with duration, 
		for(String msg:alSchedules)
		{
			//log.debug("L"+this.iLocationId+" "+ msg);
			String msgSplit[] = msg.split(ConstantsDefinition.MSGBODY_DELIMITER);
			String agentPrty = msgSplit[ConstantsDefinition.AGENTID_INDEX].replace("T", "");
			Integer iPrty = Integer.parseInt(agentPrty);	


			//Check if mapSchedule already contains an entry for that duration
			if(mapSchedules.containsKey(iPrty))
			{
				//Should not occur, because we are assuming agents to be of the same prty
				log.error("L"+this.iLocationId+" Duplicate instances with same prty "+iPrty);
			}
			else
			{
				//Create an arraylist, add msg to it and add the entry, value to the map
				ArrayList<String> listMsg = new ArrayList<String>();
				listMsg.add(msg);
				mapSchedules.put(iPrty, listMsg);
			}
		}//end for all msg
		return mapSchedules;
	}


	/**
	 *  The agent speed is maintained in the hashmap for agent properties. The treemap maintains entries in the
	 *  natural sorting order of the key field. Since we want the sorting of the speeds in the descending order
	 *  hence we are making the key of the treemap as (MAX_SPEED - speed)
	 * @param alSchedules : list of schedule comprising  0#2017-04-24:1700%2017-04-24:1806#-1#T1 status#timePeriod#DW#AgentId
	 * @return
	 */

	private TreeMap<Integer, ArrayList<String>> sortBySpeed(ArrayList<String> alSchedules)
	{
		//Keep a hashmap indexed by agent prty
		TreeMap<Integer, ArrayList<String>> mapSchedules = new TreeMap<Integer, ArrayList<String>>();


		//Populate the sortedhashmap indexed with duration, 
		for(String msg:alSchedules)
		{
			//log.debug("L"+this.iLocationId+" "+ msg);
			String msgSplit[] = msg.split(ConstantsDefinition.MSGBODY_DELIMITER);
			Integer iSpeed = hmTravellerProperty.get(msgSplit[ConstantsDefinition.AGENTID_INDEX]);	


			//Check if mapSchedule already contains an entry for that duration
			if(mapSchedules.containsKey((ConstantsDefinition.MAX_SPEED -iSpeed)))
			{
				//Could could occur, because we are assuming agents to be of the same speed. So we will generate an incremental speed 
				//and add it to the map.
				mapSchedules.get((ConstantsDefinition.MAX_SPEED -iSpeed)).add(msg);
			}
			else
			{
				//Create an arraylist, add msg to it and add the entry, value to the map
				ArrayList<String> listMsg = new ArrayList<String>();
				listMsg.add(msg);
				mapSchedules.put((ConstantsDefinition.MAX_SPEED -iSpeed), listMsg);
			}
		}//end for all msg
		log.debug(alSchedules+" sorted by Speed is: "+mapSchedules);

		return mapSchedules;
	}
	/*
	 * This function will sort the scedules by the length of the agent such that shortest length agent is first.
	 * ie in ascending order of length
	 */
	private Map<Integer, ArrayList<String>> sortByLength(ArrayList<String> alSchedules) 
	{
		//Keep a hashmap indexed by agent lenght
		TreeMap<Integer, ArrayList<String>> mapSchedules = new TreeMap<Integer, ArrayList<String>>();


		//Populate the sortedhashmap indexed with duration, 
		for(String msg:alSchedules)
		{
			//log.debug("L"+this.iLocationId+" "+ msg);
			String msgSplit[] = msg.split(ConstantsDefinition.MSGBODY_DELIMITER);
			Integer iLength = hmTravellerProperty.get(msgSplit[ConstantsDefinition.AGENTID_INDEX]);	


			//Check if mapSchedule already contains an entry for that duration
			if(mapSchedules.containsKey(iLength))
			{
				//Could could occur, because we are assuming agents to be of the same length. 
				mapSchedules.get(iLength).add(msg);
			}
			else
			{
				//Create an arraylist, add msg to it and add the entry, value to the map
				ArrayList<String> listMsg = new ArrayList<String>();
				listMsg.add(msg);
				mapSchedules.put(iLength, listMsg);
			}
		}//end for all msg
		log.debug(alSchedules+" sorted by Length is: "+mapSchedules);
		return mapSchedules;
	}
	private ArrayList<String> generateSortedSchedule(TreeMap<Integer, ArrayList<String>> sortedSchedules) 
	{

		//This will maintain the schedule which is being generated by the location.
		ArrayList<String>alTmpProposedSchedule = new ArrayList<String>();

		alProposedSchedule.clear();

		//iterate thru each date-key
		for(Integer prtyList:sortedSchedules.keySet())
		{
			//get schedule-request to be allocated
			ArrayList<String> alRequestSchedule =  sortedSchedules.get(prtyList);

			//alRequestSchedule can have only one entry.
			for(int indexRequestSchedule = 0; indexRequestSchedule < alRequestSchedule.size(); indexRequestSchedule++)
			{
				log.debug(alRequestSchedule.get(indexRequestSchedule));
				String timePeriod = alRequestSchedule.get(indexRequestSchedule).split(ConstantsDefinition.MSGBODY_DELIMITER)[ConstantsDefinition.PERIOD_INDEX];
				log.debug("L"+this.iLocationId+" split timePeriod"+timePeriod);

				boolean bOverlap = checkOverlap(timePeriod, alProposedSchedule, -1);

				log.debug("L"+this.iLocationId+" bOverlap "+bOverlap );
				while(bOverlap)
				{
					log.debug("L"+this.iLocationId+" overlap"+ bOverlap);
					//The elements should be scheduled after the overlap period. The revised schedule could still overlap.
					//so keep checking.
					//In this case, the overlap is with the proposed schedule.
					//Since the proposed schedule is being created by the earliest start date,
					//it implies, that the current timeperiod is overlapping with some event, which
					//was actually scheduled earlier. In this case, find the event with the smaller
					//duration and schedule it first and then the other later.
					//log.debug("NOT HANDLEDDDDDDDDDDDDDDDDD Index:"+iOverLapIndex);
					//get the constraint with which the overlap is identified. The overlap
					//can be resolved by ensuring that the start of the overlap and the start 
					//of the time period are separated by TPP+GAP. If the separation exists then
					//add the time-period as is, else update the timeperiod to allow for the separation
					String overlappingAllocation = alProposedSchedule.get(iOverLapIndex);

					int calStartTime=0, calAllocStartTime = 0;
					try
					{
						calStartTime = Integer.parseInt(timePeriod.split(ConstantsDefinition.TIMEDURATION_DELIMITER)[0]);
						calAllocStartTime = Integer.parseInt(overlappingAllocation.split(ConstantsDefinition.MSGBODY_DELIMITER)[ConstantsDefinition.PERIOD_INDEX].split(ConstantsDefinition.TIMEDURATION_DELIMITER)[0]);
					}
					catch(Exception e)
					{
						log.error("L"+this.iLocationId+ " Error parsing "+timePeriod.split(ConstantsDefinition.TIMEDURATION_DELIMITER)[0]);
						e.printStackTrace();
					}
					long lDiff = calAllocStartTime- calStartTime;

					if(Math.abs(lDiff) < TPP+GAP)
					{
						//update the timePeriod
						calStartTime = calAllocStartTime;
						calStartTime+= (TPP+GAP);

						int calEndTime = 0;

						try
						{
							calEndTime = Integer.parseInt(timePeriod.split(ConstantsDefinition.TIMEDURATION_DELIMITER)[1]);
							calEndTime += (TPP+GAP);
						}

						catch(Exception e)
						{
							log.error("L"+this.iLocationId+ " Error parsing "+timePeriod.split(ConstantsDefinition.TIMEDURATION_DELIMITER)[1]);
						}

						timePeriod = String.valueOf(calStartTime)
								.concat(ConstantsDefinition.TIMEDURATION_DELIMITER)
								.concat(String.valueOf(calEndTime));

					}
					else
					{
						log.debug("L"+this.iLocationId+" Gap exists between the constraint and timeperiod. Exiting");
						//break;
					}



					bOverlap = checkOverlap(timePeriod, alProposedSchedule, iOverLapIndex);
				}			

				//Add the schedules to the tmpProposedSchedule
				log.debug("L"+this.iLocationId+" Adding::"+ alRequestSchedule.get(indexRequestSchedule).split(ConstantsDefinition.MSGBODY_DELIMITER)[ConstantsDefinition.STATUS_INDEX].concat(ConstantsDefinition.MSGBODY_DELIMITER).concat(timePeriod).concat(ConstantsDefinition.MSGBODY_DELIMITER).concat( alRequestSchedule.get(indexRequestSchedule).split(ConstantsDefinition.MSGBODY_DELIMITER)[ConstantsDefinition.AGENTID_INDEX]));
				alProposedSchedule.add(alRequestSchedule.get(indexRequestSchedule).split(ConstantsDefinition.MSGBODY_DELIMITER)[ConstantsDefinition.STATUS_INDEX].concat(ConstantsDefinition.MSGBODY_DELIMITER).concat(timePeriod).concat(ConstantsDefinition.MSGBODY_DELIMITER).concat( alRequestSchedule.get(indexRequestSchedule).split(ConstantsDefinition.MSGBODY_DELIMITER)[ConstantsDefinition.AGENTID_INDEX]));
			}

		}//end for duration


		return alTmpProposedSchedule; //This is going to return an empty array. Keeping it to retain similarity with older code.

	}
	/**
	 * This function will create a schedule to attempt to maintain the schedules as much as possible.
	 * The logic to be followed is yet to be captured here.
	 * @param alSchedules: Each string is of the format status#startTime%endTime#AgentId#DWcount	 
	 * */
	private ArrayList<String> maximiseSatisfy(ArrayList<String> alSchedules) {
		// TODO Auto-generated method stub
		return null;
	}

	/**
	 *  This function will create a schedule by shortest processing time
	 *  The method will look into each request, sorted by start time. And will keep allocating
	 *  each request, if there are no overlaps. In case of overlaps, the solution which ensures
	 *  least lateness will be adopted. The optimality of this solution remains to be proven.
	 *  
	 * @param alSchedules: Each string is of the format status#startTime%endTime#AgentId#DWcount
	 * @return arrayList of revised schedules in the form timePeriod#agentId .
	 */
	private ArrayList<String> sptSchedule(ArrayList<String> alSchedules) 
	{
		//Sort all the schedules by the shortest processing time. 
		//Beginning with the first, keep allocating each request till it overlaps with
		//either a constraint or previous allocation. If the previous allocation had the same starting 
		//time, then shift the conflicting allocation after completion of the previous one.  
		TreeMap<Integer, ArrayList<String>>sortedSchedules = new TreeMap<Integer, ArrayList<String>>();
		sortedSchedules.putAll(sortByStartingTime(alSchedules));

		//This will maintain the schedule which is being generated by the location.
		ArrayList<String>alTmpProposedSchedule = new ArrayList<String>();

		//iterate thru each date-key
		for(Integer startingDate:sortedSchedules.keySet())
		{
			//get the list of msgs which begin on the same date
			ArrayList<String> alSameStartDateRequests = sortedSchedules.get(startingDate);
			//			log.debug(alSameStartDateRequests);

			//schedule the same date events 
			positionRequests(alSameStartDateRequests, alTmpProposedSchedule);
		}//end for duration

		log.debug("alProposedSchedule:"+alTmpProposedSchedule);
		return alTmpProposedSchedule;
	}
	/**
	 *  This function will work according to optimiseSchedule strategy. It will 
	 *  receive a set of requests which have the same start date. It will check 
	 *  and try to optimally schedule them wrt the already scheduled events.
	 *  
	 *  TODO how and when to check for constraints. 
	 * @param alSameStartDateRequests: list of msgs with the same startdate 
	 * @param alProposedSchedule: list of msgs scheduled till now. This is not the constraints
	 */
	private void positionRequests(ArrayList<String> alSameStartDateRequests, ArrayList<String> alProposedSchedule) 
	{
		//First arrange the events wrt time durations. for events with same start time, the shortest-processing-time-first
		//gives the best solution.
		TreeMap<Integer, ArrayList<String>>mapSorted = sortByProcessingTime(alSameStartDateRequests);
		//SimpleDateFormat sdf = new SimpleDateFormat(ConstantsDefinition.DATE_FORMAT);

		log.debug("L"+this.iLocationId+ " sorted map"+mapSorted);
		//If one element from alSameStartDateRequest overlaps with the alProposedSchedule then all overlap, because they have the same starttime
		String smallestProcessingTimeElement = mapSorted.get(mapSorted.firstKey()).get(0);
		String timePeriod = smallestProcessingTimeElement.split(ConstantsDefinition.MSGBODY_DELIMITER)[ConstantsDefinition.PERIOD_INDEX];

		boolean bOverlap = checkOverlap(timePeriod, alProposedSchedule,-1);
		boolean bConstraintOverlap = checkConstraintOverlap(timePeriod);
		log.debug("L"+this.iLocationId+" Overlap >>"+bOverlap+ " overlap with finalised duration:"+bConstraintOverlap);
		//If overlap exists then
		while(bOverlap || bConstraintOverlap)
		{
			log.debug("L"+this.iLocationId+" overlap"+ bOverlap +" or ConstraintOverlap "+bConstraintOverlap);
			//The elements should be scheduled after the overlap period. The revised schedule could still overlap.
			//so keep checking.
			if(bConstraintOverlap)
			{

				//get the constraint with which the overlap is identified. The overlap
				//can be resolved by ensuring that the start of the overlap and the start 
				//of the time period are separated by TPP+GAP. If the separation exists then
				//add the time-period as is, else update the timeperiod to allow for the separation
				Constraint overlappingConstraint = alConstraints.get(iOverlapConstraintIndex);
				log.debug("L"+this.iLocationId+" overlapping constraint:"+overlappingConstraint+" with timePeriod "+timePeriod);
				//Calendar calStartTime = Calendar.getInstance();
				Integer calStartTime =  Integer.parseInt(timePeriod.split(ConstantsDefinition.TIMEDURATION_DELIMITER)[0]);
				/*try
				{
					calStartTime.setTime(sdf.parse(timePeriod.split(ConstantsDefinition.TIMEDURATION_DELIMITER)[0]));
				}
				catch(Exception e)
				{
					log.error("L"+this.iLocationId+" Error parsing "+timePeriod.split(ConstantsDefinition.TIMEDURATION_DELIMITER)[0]);
				}
				 */
				//long lDiff = overlappingConstraint.getStartDate().getTime() - calStartTime.getTimeInMillis();
				long lDiff = overlappingConstraint.getStartDate() - calStartTime;

				if(Math.abs(lDiff) < TPP+GAP)
				{
					//update the timePeriod
					calStartTime = overlappingConstraint.getStartDate();
					calStartTime += TPP+GAP;

					Integer calEndTime = Integer.parseInt(timePeriod.split(ConstantsDefinition.TIMEDURATION_DELIMITER)[1]);

					calEndTime += TPP+GAP;

					log.debug("L"+this.iLocationId+" Original timePeriod"+timePeriod);
					timePeriod = String.valueOf(calStartTime)
							.concat(ConstantsDefinition.TIMEDURATION_DELIMITER)
							.concat(String.valueOf(calEndTime));
					log.debug("L"+this.iLocationId+" Revised timePeriod"+timePeriod);
				}
				else
				{
					log.debug("L"+this.iLocationId+" Gap exists between the constraint and timeperiod. Exiting");
					break;
				}

			}
			if (bOverlap)
			{
				//In this case, the overlap is with the proposed schedule.
				//Since the proposed schedule is being created by the earliest start date,
				//it implies, that the current timeperiod is overlapping with some event, which
				//was actually scheduled earlier. In this case, find the event with the smaller
				//duration and schedule it first and then the other later.
				log.debug("L"+this.iLocationId+" NOT HANDLEDDDDDDDDDDDDDDDDD Index:"+iOverLapIndex);
				String constraint = alProposedSchedule.get(iOverLapIndex);
				log.debug("L"+this.iLocationId+" overlapping schedule:"+constraint+" with timePeriod "+timePeriod);
				String strScheduledTimePeriod = constraint.split(ConstantsDefinition.MSGBODY_DELIMITER)[ConstantsDefinition.PERIOD_INDEX];

				//Calendar calStartTime = Calendar.getInstance();
				Integer calStartTime =  Integer.parseInt(timePeriod.split(ConstantsDefinition.TIMEDURATION_DELIMITER)[0]);
				Integer calScheduledStartTime =  Integer.parseInt(strScheduledTimePeriod.split(ConstantsDefinition.TIMEDURATION_DELIMITER)[0]);

				long lDiff = calScheduledStartTime - calStartTime;

				if(Math.abs(lDiff) < TPP+GAP)
				{
					//update the timePeriod
					calStartTime = calScheduledStartTime;
					calStartTime += TPP+GAP;

					Integer calEndTime = Integer.parseInt(timePeriod.split(ConstantsDefinition.TIMEDURATION_DELIMITER)[1]);

					calEndTime += TPP+GAP;

					log.debug("L"+this.iLocationId+" Original timePeriod"+timePeriod);
					timePeriod = String.valueOf(calStartTime)
							.concat(ConstantsDefinition.TIMEDURATION_DELIMITER)
							.concat(String.valueOf(calEndTime));
					log.debug("L"+this.iLocationId+" Revised timePeriod"+timePeriod);
				}
				else
				{
					log.debug("L"+this.iLocationId+" Gap exists between the constraint and timeperiod. Exiting");
					break;
				}
			}

			bOverlap = checkOverlap(timePeriod, alProposedSchedule, -1);
			bConstraintOverlap = checkConstraintOverlap(timePeriod);
		}

		//Add the sortedPorcessing list to the list of proposed schedule, in that order
		//You may have to revise the starttime of the remaining elements in the sortedProcessing list
		Iterator<Integer> iterSortedMap = mapSorted.keySet().iterator();
		//Calendar nextDate = Calendar.getInstance();
		Integer nextDate = 0;
		boolean bFirst = true;
		while(iterSortedMap.hasNext())
		{
			int duration = iterSortedMap.next();
			ArrayList<String>alSameDurationSameStart = new ArrayList<String>();//events with same start n same duration
			alSameDurationSameStart.addAll(mapSorted.get(duration));

			for(String msg:alSameDurationSameStart)
			{
				log.debug(msg);
				String strTimePeriod = msg.split(ConstantsDefinition.MSGBODY_DELIMITER)[ConstantsDefinition.PERIOD_INDEX];

				if(bFirst)
				{
					bFirst = false;
					try
					{
						//nextDate.setTime(sdf.parse(strTimePeriod.split(ConstantsDefinition.TIMEDURATION_DELIMITER)[0]));
						nextDate = Integer.parseInt(strTimePeriod.split(ConstantsDefinition.TIMEDURATION_DELIMITER)[0]);
					}
					catch(Exception e)
					{
						log.error("L"+this.iLocationId+" Error parsing date string"+strTimePeriod.split(ConstantsDefinition.TIMEDURATION_DELIMITER)[0]);
					}
					log.debug("L"+this.iLocationId+" Adding::"+msg.split(ConstantsDefinition.MSGBODY_DELIMITER)[ConstantsDefinition.STATUS_INDEX].concat(ConstantsDefinition.MSGBODY_DELIMITER).concat(strTimePeriod).concat(ConstantsDefinition.MSGBODY_DELIMITER).concat(msg.split(ConstantsDefinition.MSGBODY_DELIMITER)[ConstantsDefinition.AGENTID_INDEX]));
					alProposedSchedule.add(msg.split(ConstantsDefinition.MSGBODY_DELIMITER)[ConstantsDefinition.STATUS_INDEX].concat(ConstantsDefinition.MSGBODY_DELIMITER).concat(strTimePeriod).concat(ConstantsDefinition.MSGBODY_DELIMITER).concat(msg.split(ConstantsDefinition.MSGBODY_DELIMITER)[ConstantsDefinition.AGENTID_INDEX]));
				}
				else
				{
					//Define the shifted start date and end date for the reserve request
					/*					Calendar calStartDate = Calendar.getInstance();
					calStartDate.setTime(nextDate.getTime());
					calStartDate.add(Calendar.MINUTE, GAP+TPP);

					Calendar calEndDate = Calendar.getInstance();
					calEndDate.setTime(calStartDate.getTime());
					calEndDate.add(Calendar.MINUTE, duration);*/

					Integer calStartDate = nextDate;
					calStartDate += GAP+TPP;

					Integer calEndDate = calStartDate;
					calEndDate += duration;

					log.debug("L"+this.iLocationId+" start:"+calStartDate+" end:"+calEndDate);
					/*String newSchedule = sdf.format(nextDate.getTime()).concat(ConstantsDefinition.TIMEDURATION_DELIMITER)
										.concat(sdf.format(calEndDate.getTime()).concat(ConstantsDefinition.MSGBODY_DELIMITER)
												.concat(msg.split(ConstantsDefinition.MSGBODY_DELIMITER)[ConstantsDefinition.AGENTID_INDEX]));
					 */
					String newSchedule = String.valueOf(calStartDate).concat(ConstantsDefinition.TIMEDURATION_DELIMITER)
							.concat(String.valueOf(calEndDate).concat(ConstantsDefinition.MSGBODY_DELIMITER)
									.concat(msg.split(ConstantsDefinition.MSGBODY_DELIMITER)[ConstantsDefinition.AGENTID_INDEX]));
					log.debug("L"+this.iLocationId+" newSchedule"+newSchedule);

					alProposedSchedule.add(msg.split(ConstantsDefinition.MSGBODY_DELIMITER)[ConstantsDefinition.STATUS_INDEX].concat(ConstantsDefinition.MSGBODY_DELIMITER).concat(newSchedule));
					//nextDate.setTime(calEndDate.getTime());
					nextDate = calStartDate;
					log.debug("L"+this.iLocationId+" next date:"+nextDate);
				}//end else not the first entry

			}//end for msg
		}//end while iterSortedMap

	}//end function
	/**
	 *  This function will check if the reserved timePeriod overlaps
	 *  with a constrained duration.
	 * @param timePeriod start%end
	 * @return
	 */
	private boolean checkConstraintOverlap(String timePeriod) 
	{
		//SimpleDateFormat sdf = new SimpleDateFormat(ConstantsDefinition.DATE_FORMAT);
		iOverlapConstraintIndex = INVALID;
		for(Constraint constraint:alConstraints)
		{
			String strConstrainedTimePeriod = String.valueOf(constraint.getStartDate()).concat(ConstantsDefinition.TIMEDURATION_DELIMITER)
					.concat(String.valueOf(constraint.getEndDate()));


			int iCompare = DateUtil.compareTimePeriod(timePeriod, strConstrainedTimePeriod);
			if(iCompare != ConstantsDefinition.AFTER && iCompare != ConstantsDefinition.MEETS_INVERSE)
			{
				iOverlapConstraintIndex = alConstraints.indexOf(constraint);
				return true;
			}

		}//end for
		return false;
	}

	/**
	 * This function will check if the given timePeriod overlaps with any element in the alProposedSchedule. 
	 * It is not sufficient to simply check with the last element of the alProposedSchedule. all elements must be 
	 * checked for to confirm overlap or absence of it.
	 * @param timePeriod : startPeriodTIMEPERIODLIMITERendPeriod (dates in DATE_FORMAT)
	 * @param alProposedSchedule list of msgs which have been scheduled till now.
	 * @return boolean true if overlap else false
	 */
	private boolean checkOverlap(String timePeriod, ArrayList<String> alProposedSchedule, int iStartIndex) 
	{
		for(int i=iStartIndex+1; i<alProposedSchedule.size(); i++)
		{
			String msg = alProposedSchedule.get(i);
			log.debug("L"+this.iLocationId+" Comparing "+timePeriod+" with "+msg);
			String strScheduledTimePeriod = msg.split(ConstantsDefinition.MSGBODY_DELIMITER)[ConstantsDefinition.PERIOD_INDEX];
			int iCompare = DateUtil.compareTimePeriod(timePeriod, strScheduledTimePeriod);
			if(iCompare != ConstantsDefinition.AFTER && iCompare != ConstantsDefinition.MEETS_INVERSE)
			{
				iOverLapIndex = alProposedSchedule.indexOf(msg);
				return true;
			}
		}//end for
		return false;
	}

	/**
	 * This function will take a list of schedules and sort them in the increasing order of 
	 * processing time required. Ideally the input list contains all msgs with the same startdate
	 * @param alSchedules: Each string is of the format status#startTime%endTime#AgentId#DWcount
	 */
	private TreeMap<Integer, ArrayList<String>> sortByProcessingTime(ArrayList<String> alSameStartDateRequests) 
	{
		//Keep a hashmap indexed by duration
		TreeMap<Integer, ArrayList<String>> mapSchedules = new TreeMap<>();

		//SimpleDateFormat sdf = new SimpleDateFormat(ConstantsDefinition.DATE_FORMAT);

		//Populate the sortedhashmap indexed with duration, 
		for(String msg:alSameStartDateRequests)
		{
			String msgSplit[] = msg.split(ConstantsDefinition.MSGBODY_DELIMITER);
			String timeDurationSplit[] = msgSplit[ConstantsDefinition.PERIOD_INDEX].split(ConstantsDefinition.TIMEDURATION_DELIMITER);
			int duration = ConstantsDefinition.INVALID;
			try 
			{
				//	Calendar calStartTime = Calendar.getInstance();
				//	calStartTime.setTime(sdf.parse(timeDurationSplit[0]));
				Integer calStartTime = Integer.parseInt(timeDurationSplit[0]);

				//	Calendar calEndTime = Calendar.getInstance();
				//	calEndTime.setTime(sdf.parse(timeDurationSplit[1]));
				Integer calEndTime = Integer.parseInt(timeDurationSplit[1]);

				//duration = (int)TimeUnit.MILLISECONDS.toMinutes(calEndTime.getTimeInMillis() - calStartTime.getTimeInMillis());
				duration = calEndTime - calStartTime;
			}
			catch(Exception e)
			{
				log.error("Exception in parsing date "+timeDurationSplit[0]);
			}
			if(duration!=ConstantsDefinition.INVALID)
			{
				//Check if mapSchedule already contains an entry for that duration
				if(mapSchedules.containsKey(duration))
				{
					//Insert into the entry
					mapSchedules.get(duration).add(msg);
				}
				else
				{
					//Create an arraylist, add msg to it and add the entry, value to the map
					ArrayList<String> listMsg = new ArrayList<>();
					listMsg.add(msg);
					mapSchedules.put(duration, listMsg);
				}
			}//end if !duration

		}//end for all msg
		return mapSchedules;
	}

	/**
	 * This function will take a list of schedules and sort them in the increasing order of 
	 * starting time required. 
	 * @param alSchedules: Each string is of the format status#startTime%endTime#AgentId#DWcount
	 */
	private TreeMap<Integer, ArrayList<String>> sortByStartingTime(ArrayList<String> alSchedules) 
	{

		//Keep a hashmap indexed by duration
		TreeMap<Integer, ArrayList<String>> mapSchedules = new TreeMap<Integer, ArrayList<String>>();

		SimpleDateFormat sdf = new SimpleDateFormat(ConstantsDefinition.DATE_FORMAT);

		//Populate the sortedhashmap indexed with duration, 
		for(String msg:alSchedules)
		{
			String msgSplit[] = msg.split(ConstantsDefinition.MSGBODY_DELIMITER);
			String timeDurationSplit[] = msgSplit[ConstantsDefinition.PERIOD_INDEX].split(ConstantsDefinition.TIMEDURATION_DELIMITER);

			try 
			{
				//Calendar calStartTime = Calendar.getInstance();
				int calStartTime = Integer.parseInt(timeDurationSplit[0]);
				//calStartTime.setTime(sdf.parse(timeDurationSplit[0]));

				//Check if mapSchedule already contains an entry for that duration
				if(mapSchedules.containsKey(calStartTime))
				{
					//Insert into the entry
					mapSchedules.get(calStartTime).add(msg);
				}
				else
				{
					//Create an arraylist, add msg to it and add the entry, value to the map
					ArrayList<String> listMsg = new ArrayList<String>();
					listMsg.add(msg);
					mapSchedules.put(calStartTime, listMsg);
				}
			}
			catch(Exception e)
			{
				log.error("Exception in parsing date "+timeDurationSplit[0]);
			}

		}//end for all msg

		log.debug("Sorted map"+mapSchedules.keySet());
		return mapSchedules;
	}//end function

	/**
	 * This function will iterate through the list of finalises received and will push it into the list of constraints.
	 *  It will also remove the entries for the finalises from the alAgents and corresponding status list.
	 */
	private void updateConstraints() 
	{
		for(String strFinalise:alFinalise)
		{
			//Get the agent Id from the msg, remove the preceding "T" from it.
			int iAgentId = Integer.parseInt(strFinalise.split(ConstantsDefinition.MSGBODY_DELIMITER)[ConstantsDefinition.AGENTID_INDEX].replace(ConstantsDefinition.TRAVELLER, ""));
			int iAgentIndex = alAgents.indexOf(iAgentId);
			log.debug("Removing the "+iAgentIndex +" from status and list of agents");
			if(iAgentIndex != -1)
			{
				alAgents.remove(iAgentIndex);
				alExpectedStatus.remove(iAgentIndex);
			}
			//Update the constraints list
			String strPeriod = strFinalise.split(ConstantsDefinition.MSGBODY_DELIMITER)[ConstantsDefinition.PERIOD_INDEX];
			String startPeriod = strPeriod.split(ConstantsDefinition.TIMEDURATION_DELIMITER)[0];
			String endPeriod = strPeriod.split(ConstantsDefinition.TIMEDURATION_DELIMITER)[1];
			SimpleDateFormat sdf = new SimpleDateFormat(ConstantsDefinition.DATE_FORMAT);

			try
			{
				Constraint constraint = new Constraint();
				constraint.setStartDate(Integer.parseInt(startPeriod));
				constraint.setEndDate(Integer.parseInt(endPeriod));
				constraint.setAgentId(iAgentId);
				alConstraints.add(constraint);
			}
			catch(Exception e)
			{
				log.error("parsing exception ");
				e.printStackTrace();
			}
		}
	}

	/**
	 *  This method will iteration through the listRejects and will
	 *  remove the entries from the known agents and their corresponding status
	 *  
	 *  It should also iterate through the list of previous schedule and delete entries from it. TODO
	 */
	private void removeRejects() 
	{
		for(String strReject:alRejections)
		{
			//Get the agent Id from the msg, remove the preceding "T" from it.
			int iAgentId = Integer.parseInt(strReject.split(ConstantsDefinition.MSGBODY_DELIMITER)[ConstantsDefinition.AGENTID_INDEX].replace(ConstantsDefinition.TRAVELLER, ""));
			int iAgentIndex = alAgents.indexOf(iAgentId);
			log.debug("Removing the "+iAgentIndex +" from status and list of agents");
			if(iAgentIndex >= 0)
			{
				alAgents.remove(iAgentIndex);
				alExpectedStatus.remove(iAgentIndex);
			}
			//TODO Remove from scheduled list.
		}

	}

	/**
	 * This function will take in the message received from the PO.
	 * Ideally the PO is responsible for the validation of the msgs dispatched.
	 * However in this function, the following should be checked.
	 * a) There is one and only one msg from every known agent
	 * b) The receiver of the msg is the current agent
	 * c) The status of the msg received is an expected msg. 
	 *
	 * @param alRxdMsg: Received list of Messages from traveller to be processed.
	 */
	private boolean validateMsg(List<Message> alRxdMsg) 
	{
		log.debug("validate Msg"+alRxdMsg);

		ArrayList<Integer> alRxdMsgFromAgent = new ArrayList<Integer>(); //temp list to maintain the list of all agents from whom msg is rxd in current tic. Will be cleared at every tic.

		for(Message msg:alRxdMsg)
		{
			if(msg.getReceiver().equalsIgnoreCase((ConstantsDefinition.LOCATION).concat(String.valueOf(this.iLocationId))))
			{
				//Check that the sender is a Traveller agent
				if(msg.getSender().startsWith(ConstantsDefinition.TRAVELLER))
				{
					//Check if the sender is a known agent. If yes, no other msg received from it.

					int iSentAgentId = Integer.parseInt(msg.getSender().replace(ConstantsDefinition.TRAVELLER, "").split(ConstantsDefinition.SENDER_DELIMITER)[0]);

					if(alAgents.contains(iSentAgentId))
					{
						log.debug("L"+this.iLocationId+" alAgents contains "+iSentAgentId);
						//Check whether another msg is already been received from it.
						if(alRxdMsgFromAgent.contains(iSentAgentId))
						{
							log.error("L"+this.iLocationId+" Multiple msgs from "+msg.getSender());
							log.debug("L"+this.iLocationId+" validate Msg: Returning false");
							return false;
						}
						//get the index at which the agent is present and set the expected msgtype 
						//at that index
						int index = alAgents.indexOf(iSentAgentId);
						updateExpectedStatus(index);
						//alExpectedStatus.add(index, ConstantsDefinition.ACCEPT);

					}//if known agent
					else //new agent
					{
						//						log.debug("alAgents not contains "+iSentAgentId);

						//Add the new agent to the list and set the expected msg to RESERVE
						alAgents.add(iSentAgentId);
						alExpectedStatus.add(ConstantsDefinition.RESERVE);
					}

					alRxdMsgFromAgent.add(iSentAgentId);

					//Check whether msg received is a valid expected msg
					if(!checkExpectedMsg(msg))
					{
						log.error("L"+this.iLocationId+" Unexpected msgs from " + msg.getSender() + " Rxd "
								+ msg.getBody()/* +" instead of "+alAgents.get(iSentAgentId) */);
						log.debug("L"+this.iLocationId+ " validate Msg: Returning false");
						return false;
					}


				}//end if sender is traveller
				else //sender is not a traveller agent
				{
					log.error("L"+this.iLocationId+" Wrong sender. Sent by "+msg.getSender());
					log.debug("L"+this.iLocationId+" validate Msg: Returning false");
					return false;
				}

			}//end if receiver is this location
			else //received at wrong end
			{
				log.error("L"+this.iLocationId+" Wrong receiver. To be received by "+msg.getReceiver());
				log.debug("L"+this.iLocationId+" validate Msg: Returning false");
				return false;
			}


		}

		ArrayList<Integer> alRemoveAgent = new ArrayList<Integer>();

		//Check whether any known agent hasnt sent a msg
		for(int indexAgents = 0; indexAgents<alAgents.size(); indexAgents++)
		{
			//If it is a valid agent, then it should be in the received list
			if(!alRxdMsgFromAgent.contains(alAgents.get(indexAgents)))
			{
				log.debug("L"+this.iLocationId+" No msg received from "+indexAgents);
				log.debug("L"+this.iLocationId+" validate Msg: Returning false");
				alRemoveAgent.add(alAgents.get(indexAgents));
				//	return false;
			}
		}//end for indexAgents
		//remove those agents from the list
		for(int i=0; i<alRemoveAgent.size(); i++)
		{
			for (int j=0; j< alAgents.size(); j++)
			{
				if(alAgents.get(j)==alRemoveAgent.get(i))
				{
					alAgents.remove(j);

					break;
				}
			}
		}


		//Check whether any agent whose plan has been finalised has sent a msg
		for(int indexAgents = 0; indexAgents<alFinalisedAgents.size(); indexAgents++)
		{
			//If it is a valid agent, then it should be in the received list
			if(alRxdMsgFromAgent.contains(alFinalisedAgents.get(indexAgents)))
			{
				log.error("L"+this.iLocationId+" Msg received from "+alFinalisedAgents.get(indexAgents)+" which is finalised");
				log.debug("L"+this.iLocationId+" validate Msg: Returning false");
				return false;
			}
		}//end for indexAgents


		//		log.debug("validate Msg: Returning true");
		return true;
	}
	/**
	 *  This function will update the expected Status msg for the agent 
	 *  at position index.
	 *  It will compare the previous value, and based on the value, it
	 *  will change it to the expected next status value
	 * @param index : position of the agent in the alStatus
	 */
	private void updateExpectedStatus(int index) 
	{
		log.debug("update Expected status: current"+alExpectedStatus.get(index));
		Integer currentStatus = alExpectedStatus.get(index);
		alExpectedStatus.remove(index);
		if(currentStatus == ConstantsDefinition.RESERVE)
			alExpectedStatus.add(index, ConstantsDefinition.ACCEPT);
		else if (currentStatus == ConstantsDefinition.ACCEPT)
			alExpectedStatus.add(index, ConstantsDefinition.DW);
		else if (currentStatus == ConstantsDefinition.DW)
			alExpectedStatus.add(index, ConstantsDefinition.DW);
		else
			log.error("Current Status is "+currentStatus+" Expected not defined");
		log.debug("expected status set to"+alExpectedStatus.get(index));

	}

	/**
	 *  This function will check the message body to see if the msg received 
	 *  is expected or "reject', Any other msg is thrown out as unexpected.
	 *  
	 * @param msg: Body of the msg will be status#time-period
	 * @return
	 */
	private boolean checkExpectedMsg(Message msg) 
	{
		int iAgentId = Integer.parseInt(msg.getSender().replace(ConstantsDefinition.TRAVELLER, "").split(ConstantsDefinition.SENDER_DELIMITER)[0]);
		Integer status = Integer.parseInt(msg.getBody().split(ConstantsDefinition.MSGBODY_DELIMITER)[ConstantsDefinition.STATUS_INDEX]);
		//Get the index from the alAgents, and find the status for the corresponding index in the alStatus
		/*if(alAgents.contains(iAgentId))
		{
			int iAgentIndex = alAgents.indexOf(iAgentId);
			Integer expectedStatus = alExpectedStatus.get(iAgentIndex);
			log.debug("expected "+expectedStatus+" rxd"+status);
			//Check if the received status is REJECT
			if(status != ConstantsDefinition.REJECT && status != expectedStatus)
			{
				log.error("Unexpected msg "+status+" received from "+iAgentId);
				log.debug("chkExpectedMsg: Returning false");
				return false;
			}

		}*/
		return true;
	}


	/**
	 * This function will check if the given agentId is new or known. 
	 * alAgents maintains the agentStatus with the agentId as index. So only
	 * known agents will have valid status as their value, other index positions will
	 * be filled with random values. Hence validation is done by checking the field at the
	 * agent_id index. 
	 * @param iAgentId
	 * @return
	 */
	private boolean checkKnownAgent(int iAgentId) 
	{
		if(alAgents.size()>iAgentId)
		{
			if(alAgents.get(iAgentId)!=null)
			{
				if(alAgents.get(iAgentId)==ConstantsDefinition.RESERVE
						||alAgents.get(iAgentId)==ConstantsDefinition.ACCEPT
						||alAgents.get(iAgentId)==ConstantsDefinition.DW)
					return true;
			}
		}	

		return false;
	}

	public int getLocationId() {
		return iLocationId;
	}

	public void setLocationId(int iLocationId) {
		this.iLocationId = iLocationId;
	}

	//@Override
	/*public void run() {
		alRxdMsg = Collections.synchronizedList(alRxdMsg);

		synchronized (alRxdMsg) 
		{
			//Check if the msg is sent by a known agent or new agent
			if (validateMsg(alRxdMsg))
				processMsg(alRxdMsg);		
		}

	}
	 */


}
