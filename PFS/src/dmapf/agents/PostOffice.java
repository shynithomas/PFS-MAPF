package dmapf.agents;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import dmapf.constants.ConstantsDefinition;
import dmapf.model.Message;


/**
 * This class will be used to collect the requests and responses from
 * the Location and Travelers. PO will know the total number of Location
 * and Travelers and hence in each cycle will wait till messages are received from 
 * them. It will also be responsible for collating all msgs for one agent and 
 * sending it as a single block to that agent.
 * 
 * Initially, only the number of travellers are known, when the PO agent 
 * receives the reserve request from the agents, it will collate all the 
 * requests and identify the number of locations it has to go to and
 * then it will check if those location agents exists, if not, it will create them.
 * 
 * @author st
 *
 */
public class PostOffice
{
	private ArrayList<LocationAgent> alLocationAgents = new ArrayList<>(); //list of location agents instantiated.
	private ArrayList<Travelers> alTravellers = new ArrayList<Travelers>();
	private ArrayList<Integer> alStatus = new ArrayList<Integer>(); //This will maintain the status of each locationAgent
	private int iTickCounter = ConstantsDefinition.ONE; //Time ticks counter
	private int iNoTravellerAgents; //set during init
	private int iNoLocationAgents; //This will have to be dynamically updated based on the msgs sent to,
	boolean bRxFromTraveller = true; //maintain whether comm is expected from traveler or location. Comm is init from Traveller.
	public static final Logger log = Logger.getLogger(PostOffice.class);
	private ArrayList<Integer> alRxdMsgFrom = new ArrayList<Integer>(); //list of agents from which msg has been rxd in this cycle.
	private ArrayList<ArrayList<Message>> alRxdMsgsFromAgents = new ArrayList<ArrayList<Message>>();//collection of all msgs rxd from agents in a cycle
	private HashMap<String, ArrayList<Message>> mapMsgsForTx = new HashMap<String, ArrayList<Message>>();//collection of msgs to be tx in a cycle, indexed by receiver name
	private int iDWSize; //size of the Deliberation Window. to be set to Location agents.
	long time;
	private long totalAgentCost=0 , finalisedAgentInitialCost = 0;
	private HashMap<Integer, Long> hmAgentCost = new HashMap<Integer, Long>();
	private long timeNano;
	private int iFinalisedCount=0, iTerminatedCount = 0, iTotalTravellers=0;
	boolean bFinalisedRx = false;
	private int iScheduleStrategy;
	public PostOffice(int noTravellers, int iDW, int iScheduleStrat)
	{
		iNoTravellerAgents = noTravellers;
		iTotalTravellers = noTravellers;
		iScheduleStrategy = iScheduleStrat;
		this.iDWSize = iDW;
		PropertyConfigurator.configure("resources/config-properties/log4j.properties");
	}

	public void setTravellers(ArrayList<Travelers> listTravellers)
	{
		alTravellers.addAll(listTravellers);
	}

	/**
	 *  This function will receive the message from the Travellers or Location agents. A boolean
	 *  variable maintains the state whether the comm rx is expected to be from Traveller or Location. 
	 *  This function is invoked by an agent, and hence the parameter will be the list of all msgs
	 *  that the agent wants to send to all the other parties. The PO will wait till it has been invoked 
	 *  by all agents in that cycle. After that only it will start collecting the received msgs, will 
	 *  validate them, and then will collate and package them to be sent to individual agents.
	 *  
	 *  This function will also maintain the iteration count. The count will increment after a msg is rxd
	 *  from Traveller, sent to Location and response rxd from Location and sent to Traveller. 
	 *       
	 *  @param alRxdMsg: list of all messages from an agent.
	 */
	public void receiveMessage(ArrayList<Message> alRxdMsg)
	{
		log.debug("receive Msg:"+ bRxFromTraveller);
		//Check if msg is expected from Traveller
		String agentType = bRxFromTraveller?ConstantsDefinition.TRAVELLER:ConstantsDefinition.LOCATION;

		if (validateMsg(alRxdMsg, agentType))
		{
			//Collect the msglist into the tmp store
			alRxdMsgsFromAgents.add(alRxdMsg);

			//check if msgs have been rxd from all the agents expected from
			if(allMsgsRxd(agentType))
			{
				//aggregate the msgs, and start sending to corresponding location agents.
				aggregate();

				//reset the bRxFromTraveller
				bRxFromTraveller = !bRxFromTraveller;

				//clear up the tmp store of received msgs.
				alRxdMsgsFromAgents.clear();

				//increment the time counter
				if(agentType.equalsIgnoreCase(ConstantsDefinition.LOCATION))
					iTickCounter++;

				//send the msgs to respective Agents
				transmitMessage(agentType);




				log.debug(bRxFromTraveller);
			}//end if all msgs have been received
		}//end if msg is valid	 
	}

	/**
	 * This function will check the size of alRxdMsgsFrom with the expected number of 
	 * agents of type agentType. If the sizes are same, it will return true else false;
	 * TODO:Check if instead of verifying with the count, the verification has to be 
	 * with the actual agent ids expected. 
	 * @param agentType : T or L
	 * @return 
	 */
	private boolean allMsgsRxd(String agentType) 
	{
		log.debug("allMsgRxd from "+agentType);
		if(agentType.equalsIgnoreCase(ConstantsDefinition.LOCATION))
		{
			log.debug("Expecting "+alLocationAgents.size()+ " rxd "+alRxdMsgFrom.size());
			if(alRxdMsgFrom.size() == alLocationAgents.size())
				return true;
		}
		else
		{
			log.debug("Expecting "+iNoTravellerAgents+ " rxd "+alRxdMsgFrom.size());
			if(alRxdMsgFrom.size() == iNoTravellerAgents)
				return true;
		}
		log.debug("allMsgRxd:false");
		return false;
	}

	/**
	 *  This function will transmit the collected and aggregated messages to all
	 *  the respective agents. The messages to be transmitted are in mapMsgsForTx.
	 *  They are indexed by receiver id. Each index value is prepended with either
	 *  T or L followed by agentId. 
	 *  
	 *  @param agentType: type of the sender of the msg
	 */
	private void transmitMessage(String senderAgentType) 
	{
		//Clear the rxdMsgFrom 
		alRxdMsgFrom.clear();
		//If the sender is the Traveller agent, then the transmit has to happen to Location
		if(senderAgentType.equalsIgnoreCase(ConstantsDefinition.TRAVELLER))
		{
			//transmit to location agent
			transmitToLocation();
		}
		else
		{
			//transmit to traveller agent
			transmitToTraveller();
		}
	}//end function


	/**
	 * 	This function transmits the msg from the Traveller agent to the
	 * Location agent. If the transmissions are being made to L agents, then maintain the list of
	 *  the agent Ids, if a REJECT message is being sent, then it need not be maintained 
	 *  in the list to check if response is received.
	 */
	private void transmitToLocation()
	{
		log.debug("transmit to Location"+mapMsgsForTx.keySet());
		boolean bDeleteAgent = true;
		ArrayList<Integer>alDeleteAgents = new ArrayList<Integer>();
		log.debug("location agents"+alLocationAgents.size()+" "+mapMsgsForTx.keySet());

		for(String agent:mapMsgsForTx.keySet())
		{
			boolean bAgentExists = false;

			//Check if the agent exists, else create it
			int agentId = Integer.parseInt(agent.replace(ConstantsDefinition.LOCATION, ""));

			for (LocationAgent locAgent:alLocationAgents)
			{
				//				log.debug("Existing Agent"+locAgent.getLocationId()+" with "+agentId);

				if (locAgent.getLocationId() == agentId)
				{
					bAgentExists = true;

					//Check if the msgs are only REJECT. If so, mark the agent for deletion
					ArrayList<Message> msgList = mapMsgsForTx.get(agent);
					bDeleteAgent = true;

					for(Message msg:msgList)
					{
						if(Integer.parseInt(msg.getBody().split(ConstantsDefinition.MSGBODY_DELIMITER)[ConstantsDefinition.STATUS_INDEX])!=ConstantsDefinition.REJECT)
						{
							bDeleteAgent = false;
							break;
						}//end if not reject
					}//end for all msg

					if(bDeleteAgent)
						alDeleteAgents.add(agentId);
					//		log.debug("Transmitted to location");
					//transmit the msg to the locationAgent
					locAgent.receiveMessage(mapMsgsForTx.get(agent));
					break;
				}//end if agentId
			}//end for all location agents

			//log.debug("Agent Exists"+bAgentExists);
			if(!bAgentExists)
			{
				//	log.debug("Create Agent"+agentId);
				//Create and add to the list
				LocationAgent locAgent = new LocationAgent(agentId, iDWSize, this, iScheduleStrategy);
				alLocationAgents.add(locAgent);

				log.debug("Transmitted to location");
				//transmit the msg
				locAgent.receiveMessage(mapMsgsForTx.get(agent));
			}

		}

		/*
		 * ArrayList<Message> listMsgFromLocation = new ArrayList<Message>();
		 * 
		 * for (LocationAgent locAgent:alLocationAgents)
		 * listMsgFromLocation.addAll(locAgent.processMsg());
		 * 
		 */
		/*ExecutorService executor = Executors.newFixedThreadPool(5);
		for (LocationAgent loc: alLocationAgents)
		{
			executor.execute(loc);
		}
		executor.shutdown();
		while (!executor.isTerminated()) {
		}
		 */

		//After completion of the transmission, delete the agents which had only REJECT msgs
		for(Integer agentId:alDeleteAgents)
		{
			//Iterate through the alLocation to find the corresponding agentId and delete it
			Iterator<LocationAgent> iter = alLocationAgents.iterator();
			while(iter.hasNext())
			{
				LocationAgent locAgent = iter.next();
				if(locAgent.getLocationId() == agentId)
					iter.remove();

			}
		}
		//receiveMessage(listMsgFromLocation);

	}

	/**
	 *  This function is invoked to transmit the msgs from the Location agent
	 *  to the Traveller agents.
	 */
	private void transmitToTraveller()
	{
		log.debug("Transmit to Traveller"+mapMsgsForTx.keySet());
		for(String agent:mapMsgsForTx.keySet())
		{
			int agentId = Integer.parseInt(agent.replace(ConstantsDefinition.TRAVELLER, ""));
			for(Travelers traveller:alTravellers)
			{
			//	log.debug(agentId+" "+traveller.getAgentId());
				if(agentId == traveller.getAgentId())
				{
					log.debug(mapMsgsForTx.get(agent));
					traveller.receive(mapMsgsForTx.get(agent));
				}
			}
		}




		/*ExecutorService executor = Executors.newFixedThreadPool(5);
		for (Travelers traveler: alTravellers)
		{
			executor.execute(traveler);
		}
		executor.shutdown();
		while (!executor.isTerminated()) {
		}
		System.out.println("Finished all threads");
		 */

	}



	/**
	 * This function will collect all the msgs received and sort them into containers
	 * based on the receiver. 
	 * The messages  are in the alRxdMsgsFromAgents, where every element is list of msgs 
	 * from same sender to different receivers.
	 */
	private void aggregate() 
	{
		//Iterate through every element in the alRxdMsgsFromAgents.
		//Each element is a list from the same sender, iterate thru the list.
		//Depending on the receiver, create ArrayList to put the msg based on the receiver.
		log.debug("aggregate");
		mapMsgsForTx.clear();

		for(ArrayList<Message>msgList:alRxdMsgsFromAgents)
		{
			for(Message msg:msgList)
			{
				//Check if the agent id is new or pre-existing and add to corresponding bin
				if(mapMsgsForTx.containsKey(msg.getReceiver()))
				{
					mapMsgsForTx.get(msg.getReceiver()).add(msg);
				}
				else
				{
					ArrayList<Message> listMsg = new ArrayList<Message>();
					listMsg.add(msg);
					mapMsgsForTx.put(msg.getReceiver(), listMsg);
				}//end else

			}//end for msg
		}//end for msglist
	}//end function

	/**
	 * This function will validate if the msg is received from the type of agent expected.
	 * Will check if the agent has sent any other msg. 
	 * 
	 * @param alRxdMsg: msgs received from an agent
	 * @param strAgentType: type of agent expected
	 * @return bool 
	 */
	private boolean validateMsg(ArrayList<Message> alRxdMsg, String agentType) 
	{
		log.debug("validate Msg from "+ agentType + alRxdMsg + alRxdMsgFrom);
		//Since all the msgs in this are from the same agent, hence only validate the first msg.
		for(Message msg :alRxdMsg)
		{
			if(!msg.getSender().startsWith(agentType))
			{
				log.error("Expected msg from "+agentType+", received from "+msg.getSender());
				return false;
			}
			
			
			int iAgentId = Integer.parseInt(msg.getSender().replace(agentType, "").split(ConstantsDefinition.SENDER_DELIMITER)[0]);
			if(alRxdMsgFrom.contains(iAgentId))
			{
				/*log.error("Already received msg from "+iAgentId+". Returning false");
				return false;*/
			}
			else
				alRxdMsgFrom.add(iAgentId);
		}
		return true;
	}

	public void generateSchedule() {
		time = System.currentTimeMillis();
		//timeNano = System.nanoTime();
		int iInitialCost = 0;
		HashMap<Integer, ArrayList<Message>> hmReceivedMsgFromTraveller = new HashMap<>();
		alRxdMsgFrom.clear();

		for(Travelers traveller:alTravellers)
		{
			//traveller.setPostOffice(postOfice);
			ArrayList<Message> alMsg = new ArrayList<>(); 
			alMsg.addAll(traveller.generateSchedule());
			hmReceivedMsgFromTraveller.put(traveller.getAgentId(), alMsg);
			alRxdMsgFrom.add(traveller.getAgentId());
			alRxdMsgsFromAgents.add(alMsg);
			iInitialCost+=traveller.getInitialCost();
		}

		sendToLocation(hmReceivedMsgFromTraveller);

		ArrayList<Message> alMsgFromTraveller= sendBackAndForth();
		
		while(alMsgFromTraveller.size()>0)
			{
			 	receiveMessage(alMsgFromTraveller);
			 	alMsgFromTraveller.clear();
			 	alMsgFromTraveller.addAll(sendBackAndForth());
			}
		
//			log.info("Completed. TIME:"+(System.currentTimeMillis()-time)+"msec Solution Cost"+totalAgentCost+ " Initial Cost:"+iInitialCost+" Finalised:"+iFinalisedCount+" Terminated:"+iTerminatedCount+" Total:"+iTotalTravellers);
		long iDifference;
		if(iFinalisedCount==alTravellers.size())
		{
			iDifference = totalAgentCost-iInitialCost;
			log.info("Completed. TIME:"+(System.currentTimeMillis()-time)+"msec Solution Cost"+totalAgentCost+ " Initial Cost:"+iInitialCost+" Finalised:"+iFinalisedCount+" Terminated:"+iTerminatedCount+" Total:"+iTotalTravellers + " FinalisedInitialCost:"+finalisedAgentInitialCost+" difference:"+iDifference);
		}
		else
			log.info("Completed. TIME:"+(System.currentTimeMillis()-time)+"msec Solution Cost"+totalAgentCost+ " Initial Cost:"+iInitialCost+" Finalised:"+iFinalisedCount+" Terminated:"+iTerminatedCount+" Total:"+iTotalTravellers + " FinalisedInitialCost:"+finalisedAgentInitialCost+" Difference:NA");
		
		
		
			
		log.debug(bRxFromTraveller);
	}

	
	private ArrayList<Message> sendBackAndForth() 
	{
		log.debug("sendBackAndForth");
		ArrayList<Message> listMsgFromLocation = new ArrayList<Message>();
		for (LocationAgent locAgent:alLocationAgents)
			listMsgFromLocation.addAll(locAgent.processMsg());
		receiveMessage(listMsgFromLocation); //This will collect, aggregate the msgs and tx to Travellers


		ArrayList<Message>alMsgFromTraveller = new ArrayList<Message>();
		for(Travelers traveller:alTravellers)
		{
			ArrayList<Message>alMsgs = traveller.processMsg();
			
			if(alMsgs!=null)
			{
				alMsgFromTraveller.addAll(alMsgs);
				log.debug("Rxd "+alMsgs.size()+" from T"+traveller.getAgentId());
			}
			else 
			{
				
				int iAgentStatus = traveller.getAgentStatus();
				log.debug("Returned null. T"+traveller.getAgentId()+" status:"+iAgentStatus);
				
				if(iAgentStatus==ConstantsDefinition.FINALIZE && !hmAgentCost.containsKey(traveller.getAgentId()))
				{
					totalAgentCost += traveller.agentCost;
					finalisedAgentInitialCost += traveller.getInitialCost();
				//	log.info("Agent:"+traveller.getAgentId()+" agentCost:"+traveller.agentCost+" initial cost:"+traveller.getInitialCost());
					hmAgentCost.put(traveller.getAgentId(), traveller.agentCost);
					iFinalisedCount++;
					log.debug("Finalised. T"+traveller.getAgentId()+ "totalAgentCost"+totalAgentCost+" finalInitCost"+finalisedAgentInitialCost);
					
				}
				else if(iAgentStatus == ConstantsDefinition.TERMINATE)
					iTerminatedCount++;
				else
					log.error("One of the message is null. AgentStatus="+iAgentStatus);
				//remove it from the expected list
				iNoTravellerAgents--;
				log.debug("Returned null. T"+traveller.getAgentId()+" status:"+iAgentStatus+"Terminated"+iTerminatedCount+" Finalised"+iFinalisedCount);
				bFinalisedRx = true;
			}
		}
		
		if(bFinalisedRx)
		{
			//log.error("If one is finalised, all others too should be finalised or terminated and msgFromTraveller should be zero. It is "+alMsgFromTraveller.size());
		}
		return alMsgFromTraveller;
	}

	private void sendToLocation(HashMap<Integer, ArrayList<Message>> hmReceivedMsgFromTraveller) 
	{
		//if(allMsgsRxd(ConstantsDefinition.TRAVELLER))
		{
			//aggregate the msgs, and start sending to corresponding location agents.
			aggregate();

			//reset the bRxFromTraveller
			bRxFromTraveller = !bRxFromTraveller;

			//clear up the tmp store of received msgs.
			alRxdMsgsFromAgents.clear();

			//increment the time counter
			/*if(agentType.equalsIgnoreCase(ConstantsDefinition.LOCATION))
				iTickCounter++;
			 */
			//send the msgs to respective Agents
			transmitMessage(ConstantsDefinition.TRAVELLER);


			log.debug(bRxFromTraveller);
		}//end if all msgs have been received	
	}

}