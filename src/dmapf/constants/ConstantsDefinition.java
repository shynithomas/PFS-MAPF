package dmapf.constants;



public class ConstantsDefinition {

	public static final int ASTAR = 0;
	public static final int GREEDY = 1;
	public static final int XCBS = 2;
	public static final int XCBSA = 3;
	public static final int XCBSA_EFF = 4;
	public static final int XCBSLA = 5;
	public static final int DMAPF = 6;


	public final static int EQUAL = 0;
	public final static int BEFORE = 1; //XXX  YYY
	public final static int AFTER = -1; //YYY XXX
	public final static int MEETS = 2;  //XXXYYY
	public final static int MEETS_INVERSE = -2; //YYYXXX
	public final static int OVERLAPS = 3; //XYXXYYY
	public final static int OVERLAPS_INVERSE = -3; //YXYYXXX
	public final static int DURING = 4; //YYYXXXXYYY
	public final static int DURING_INVERSE = -4; //XYYYXXX
	public final static int STARTS = 5; //X&YYYYYY
	public final static int STARTS_INVERSE = -5;
	public final static int FINISHES = 6;//YYYY&X
	public final static int FINISHES_INVERSE = -6;
	public final static int INVALID = -100;
	public static final int CONSISTENTPLAN = -1;
	public static final String CHANGE_INDICATOR = "'";
	public static final String HEAD_NODE = "head";
	public static final String TAIL_NODE = "tail";



	public static String DATE_FORMAT="yyyy-MM-dd:HHmm";
	public static String BLOCK_DELIMITER="#";
	public static String GRAPH_FILE_PATH ="resources/roadnetwork/jgrapht_sameLengthAndSpeed/empty-96-96.reduced.model";//"resources/roadnetwork/opt_1.txt";//	
	
	//public static String GRAPH_FILE_PATH ="resources/roadnetwork/tmp/Berlin_3_5.reduced.model";//"resources/roadnetwork/opt_1.txt";//
	public static String DATE_FORMAT1="dd-MM-yyyy";


	public static final String KMP2H="kmp2h";
	public static final String KMPH="kmph";

	public static String LONGHALT = "LH";
	public static String SHORTHALT = "SH";

	public static final String COLON = ":";
	public static final double MINS_IN_1HR = 60.0;
	public static final int ARCDESCLENGTH = 29;
	public static final int MAX_PARKING_CAP = 100000;
	public static final String EDGE_DELIMITOR = "#";
	public static final int MOST_RECENT = 0;
	public static final int MOST_DEEP = 1;
	public static final int MOST_FAT = 2;
	public static final int MOST_SINGLE = 3;

	public static final String LOCATION = "L";
	public static final String TRAVELLER = "T";

	public static final Integer RESERVE = 0;
	public static final Integer ACCEPT = 1;
	public static final Integer DW = 2;
	public static final Integer REJECT = 3;
	public static final Integer FINALIZE = 4;
	public static final Integer PROPOSE = 5;
	public static final Integer TERMINATE = 6;
	
	public static final String MSGBODY_DELIMITER = "#";
	public static final String SENDER_DELIMITER = "%";
	public static final String TIMEDURATION_DELIMITER = "%";
	


	public static final int STATUS_INDEX = 0;//Defines the position of the status field in the msg body
	public static final int PERIOD_INDEX = 1;//Defines the position of the period field in the msg body
	public static final int AGENTID_INDEX = 2;//Defines the position of the agent_id field in an updated msg body
	public static final int DWCOUNT_INDEX = 3;//Defines the position of the DW count field in an updated msg body

	public static final int DWINITCOUNT =1; //Defines the first count of DW on initialisation
	public static final int ONE = 1;


	public static final int TIME_TRAVELLED_IN_KMPH = 60;
	public static final int TIME_TRAVELLED_IN_KMP2H = 100;

	public static final int LOCATION_SCHEDULE_SPT = 1;
	public static final int LOCATION_SCHEDULE_SATISFYMAX = 2;
	public static final int LOCATION_SCHEDULE_SATISFY_PRTY = 3;
	public static final int LOCATION_SCHEDULE_SPEED = 4;
	public static final int LOCATION_SCHEDULE_LENGTH = 5;
	
	public static final Integer DW_UNINIT = -1; //Gap filler when the DW is not applicable.
	public static final String PATH_DELIMITOR = "%";
	public static final Integer MAX_SPEED = 100;
	public static Integer COL_SIZE = 0;
	
}
