import java.io.*;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.http.HtmlQuoting;



/**
 * TODO: create generic Interface for a generic parser to parse the log.
 *
 * @author Steve Siyang Wang
 * Created on 6/30/14.
 */

public class LogParser implements LogAnnotator {

	private String logSoFar; // The last log it read
	// private List<Check> checkList; // List of Doctor
	private Map<String, String> lineStructureMap; // Structure of each line in log, updated per extractInfo call
	private Map<String, String> compDecompMap; // Map of Compressor and De-compressor related message
	private Map<String, String> ignoreMap; // Map of ignoring obsolete input

	private Map<Integer, String> exceptionMap; // Map of exceptions occurred
	private Map<Integer, String> warnMap; // Map of WARN related message
	private Map<Integer, String> errorMap; // Map of ERROR related message

	private List<HashMap<String, String>> memoryList; // Map of memory-usage
	private List<String[]> timeSpanList; // Map of big time-spans
	private String compressionFormat;
	private int lineCounter; // Count the line number of the log
	private Map<String, Phase> phaseMap; // List of phases created

	private Pattern skipRegex = null;
	private Pattern exceptionRegex = null;
	private Pattern exceptionLocationRegex = null;
	private Pattern shufflePhaseRegex= null;
	private Pattern treeRegexPhaseRegex = null;
	private Pattern reduceMergePhaseRegex = null;
	private Pattern reducePhaseRegex = null;
	private Pattern writePhaseRegex = null;

	// CONSTANTS: to be put into ParseUtils
	public static final String WARN = "WARN";
	public static final String INFO = "INFO";
	public static final String ERROR = "ERROR";
	public static final String CODEC_POOL = "CodecPool";
	public static final String IGNORING = "ignoring";
	public static final String LIBRARY = "library";

	public static final String DATE = "Date";
	public static final String TIME = "Time";
	public static final String MESSAGE_TYPE = "MessageType";
	public static final String LOCATION = "Location";
	public static final String MESSAGE = "Message";
	public static final String ENTER_RETURN = System.getProperty("line.separator");
	private static final String SPACE = "\\s+";


	private static final long MAX_INTERVAL = 60000; // the interval between log time in millisecond, default 1 minute
	private static final long LOG_WINDOW_SIZE = 10000; 
	// See: http://eventuallyconsistent.net/2011/08/02/working-with-urlconnection-and-timeouts/
	private static final int TASKLOG_FETCH_CONNECTION_TIMEOUT_MILLIS = 5000; // 5 seconds
	private static final int TASKLOG_FETCH_READ_TIMEOUT_MILLIS = 10000; // 10 seconds

	private static final Log LOG = LogFactory.getLog(LogParser.class);
	private static final String DOT_XML = ".xml";
	private static final Pattern NAME_PATTERN = Pattern
			.compile(".*(job_[0-9]+_[0-9]+).*");

	//Instance of SignalDoctors: Instantiated at the constructor of LogParser
	private SpillDoctor sd;
	private MergeDoctor md;
	private Map<String, SignalDoctor> doctorMap;

	/**
	 * Static method that allow direct invoking
	 * TODO: Used for small file only, need to improve it to memory buffer
	 * @return boolean that indicate whether succeeded or not
	 */

	public LogParser(){
		logSoFar = null;
		lineStructureMap = new HashMap<String, String>(); 
		exceptionMap = new HashMap<Integer, String>();
		warnMap = new HashMap<Integer, String>();
		errorMap = new HashMap<Integer, String>();
		compDecompMap = new HashMap<String, String>();
		ignoreMap = new HashMap<String, String>();
		memoryList = new ArrayList<HashMap<String, String>>();
		timeSpanList = new ArrayList<String[]>();
		phaseMap= new HashMap<String, Phase>();

		//Initializign SignalDoctors
		initializeDoctors();
	}

	private void initializeDoctors() {
		doctorMap= new HashMap<String, SignalDoctor>();
		sd = new SpillDoctor();
		md = new MergeDoctor();
		doctorMap.put("SpillDoctor", sd);
		doctorMap.put("MergeDoctor", md);
	}

	//	public String readAndProcessLog(MRTaskAttemptInfo attemptInfo, String attemptID){
	public String readAndProcessLog(String filePath){
		//		Map<String, String> lineStructureMap = new HashMap<String, String>();
		// Read and parse from the local file directory with small file size
		InputStream in;
		StringBuilder out = new StringBuilder();
		String line;
		String previous = null;
		lineCounter = 1;

		try{
			in = new FileInputStream(new File(filePath));
			BufferedReader reader = new BufferedReader(new InputStreamReader(in));
			while ((line = reader.readLine()) != null) {
				out.append(line + ENTER_RETURN);

				//				System.out.println("The current line at which it stopped " + lineCounter);

				if(!checkSkipLine(line)){
					lineStructureMap = extractInfo(line, lineCounter, logSoFar);

					// Diagnose at the line basis, plug in checks here for future extensions
					checkCompressionLibrary(line);					
					checkTimeSpan(line,previous, lineCounter);
					checkMemoryUsage(lineStructureMap, lineCounter, line);

					checkTag(lineStructureMap);
					checkCodecPool(lineStructureMap);
					checkObsoleteOutput(lineStructureMap);

					checkWaitTime(lineStructureMap);
					
					//Ask SignalDoctors to do check
					for (SignalDoctor doctor : doctorMap.values()) {
						doctor.check(lineStructureMap, line, lineCounter);
//						System.out.println("printing from SignalDoc loop");
					}	    
				}
				previous = line;
				lineCounter++;
			}
			// Ask SignalDoctors to create Phases
			for (SignalDoctor doctor : doctorMap.values()) {
				Phase p = doctor.createPhase();
				phaseMap.put(p.getName(), p);
			}
			
			logSoFar = out.toString();
			//            System.out.println(lastLog);   //Prints the string content read from input stream
			reader.close();

		} catch (FileNotFoundException e) {
			e.printStackTrace();
			System.out.println("File not found. Re-check the URL address");
		} catch (IOException e) {
			e.printStackTrace();
		}
		return logSoFar;

		// Read and parse from HTTP request
		// See HiveMRTTaskLogProcessor for detail in HTTP Connect
	}

	/**	
	 * Return the line number as well as the wait time during the shuffle wait
	 * @param lineStructureMap2
	 * @return 
	 */

	private void checkWaitTime(Map<String, String> lineStructureMap) {
		Long waitTime = (long) 0;
		return;
	}

	/**
	 * Check line against the skip regex: true if the line is to skip
	 * @param line
	 * @return boolean 
	 */
	protected boolean checkSkipLine(String line) {
		skipRegex = Pattern.compile("(\\s*)(<)"); // User-specify the regex at which the pattern is matching
		exceptionRegex = Pattern.compile("(Exception)"); // 
		exceptionLocationRegex = Pattern.compile("(\\t)(at)");
		Matcher sm = skipRegex.matcher(line);
		Matcher em = exceptionRegex.matcher(line);
		Matcher elm = exceptionLocationRegex.matcher(line);
		if (sm.find()){
			// do something
			return true;
		}
		if (em.find() || elm.find()){
			checkException(line, lineCounter);
			return true;
		}
		if (line.isEmpty()){
			return true;
		}
		return false;
	}

	/**
	 * Put the exception in the 
	 * @param line
	 * @param counter
	 */
	private void checkException(String line, int counter) {
		exceptionMap.put(counter, line);
	}


	/**
	 * Gather all WARN and ERROR tagged log. Called Repeatedly.
	 * @param
	 * @return warnMap
	 */
            private void checkTag(Map<String, String> map){
		switch (map.get(MESSAGE_TYPE)){
		//		case WARN: warnMap.put();
		case WARN: warnMap.put(lineCounter, map.get(DATE) + " " + map.get(TIME) + " "+  map.get(LOCATION) + " " + map.get(MESSAGE) );
		case ERROR: errorMap.put(lineCounter, map.get(DATE) + " " + map.get(TIME) + " "+  map.get(LOCATION) + " " + map.get(MESSAGE) );
		}

	}



	/**
	 * TODO: make this a util or static method to allow direct access: Steve
	 * 
	 * Take in input string line, and return Map of structured info
	 * Every line is structured as below: "DATE TIME MESSAGE_TYPE LOCATION MESSAGE"
	 * @param Variable argument method, default first argument is the line
	 * @return Map<String, String> Structure
	 */
	private Map<String, String> extractInfo(Object ... args){
		String line = (String) args[0]; // default first argument is the line
		Map<String, String> map = new HashMap<String, String>();
		try{
			String[] tokens = line.split(SPACE);
			map.put(DATE, tokens[0]);
			map.put(TIME, tokens[1]);
			map.put(MESSAGE_TYPE, tokens[2]);
			map.put(LOCATION, tokens[3]);

			//			System.out.println("Print extractInfo: the token[2] + token[3] is " + tokens[2] + " "+ tokens[3]);
			String message = line.split(tokens[2] + " "+ tokens[3])[1];
			map.put(MESSAGE, message);
			//			System.out.println("Print extractInfo: the message here is " + message);
		} catch (IndexOutOfBoundsException e){
			e.printStackTrace();
			if (args.length == 3){
				int counter = Integer.parseInt((String) args[1]);
				String logTillNow = (String) args[2];
				System.out.println("The line number is " + counter);
				System.out.println("The file processed so far " + ENTER_RETURN
						+ logTillNow);
			}
		}
		return map;
	}

	/**
	 * Check the interval between two line in log, and return those
	 * @param current
	 * @param logCounter
	 * @return List of String Array with format line#, duration, message
	 */
	private List<String[]> checkTimeSpan(String current, String previous, int logCounter){
		String currentDateTime = lineStructureMap.get(DATE) + " " + lineStructureMap.get(TIME);
		String previousDateTime = null; Date d1 = null, d2 = null;
		// establish a date format
		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss,SSS");
		if (previous != null && !checkSkipLine(previous)){ 
			// Make sure previous is not null or un-parsable
			previousDateTime = extractInfo(previous).get(DATE) + " " + extractInfo(previous).get(TIME);

			try {
				d1 = format.parse(previousDateTime);
				d2 = format.parse(currentDateTime);
			} catch (ParseException e) {
				e.printStackTrace();
			}
			long diff = d2.getTime() - d1.getTime();

			if (diff > MAX_INTERVAL){
				// Construct a feedback String array with format: line#, duration, message
				String[] feedback = {Integer.toString(logCounter), Long.toString(diff), current};
				timeSpanList.add(feedback);
				return timeSpanList;
			}
			return null;
		}
		return null;
	}

	/**
	 * Check the memory-usage information reflected in the log being parsed
	 * @param List of Maps
	 * @return
	 */
	private List<HashMap<String, String>> checkMemoryUsage(Map<String, String> map, int counter, String line) {
		HashMap<String, String> memoryInfo = new HashMap<String, String>();
		// Key stored in the sequence of: Line, Rows, Memory, Message
		String message = map.get(MESSAGE);
		//		System.out.println("CheckMemoryUsage message is " + message );

		if (message.contains("used memory")){
			//			System.out.println("Print from checkMemoryUsage : ");

			Pattern numRegex = Pattern.compile("(\\d+)"); // The '\\d' for digit and '+' for one or more
			Matcher m = numRegex.matcher(message);
			List<String> mr = new ArrayList<String>();

			int c = 0;
			while (m.find()){
				c++;
				mr.add(m.group());
			}

			//			System.out.println("CheckMemoryUsage: the size of mr is " + mr.size());

			memoryInfo.put("Line", Integer.toString(counter));
			memoryInfo.put("Rows", mr.get(0));
			memoryInfo.put("Memory", mr.get(1));
			memoryInfo.put("Message", line);
			memoryList.add(memoryInfo);
		}
		return memoryList;
	}


	/**
	 * TODO: make this more robust
	 * Check the compression format
	 * @param line
	 * @return String compressionFormat
	 */
	private String checkCompressionLibrary(String line){

		if (line.contains(LIBRARY)){
			String[] splitArray = line.split(SPACE);
			compressionFormat = splitArray[splitArray.length-2];
		}
		return compressionFormat;
	}


	/**
	 * Gather all information about the compressor and decompressor
	 * @param map
	 * @return
	 */
	private Map<String, String> checkCodecPool(Map<String, String> map){
		if (map.get(LOCATION).contains(CODEC_POOL)){
			compDecompMap.put(map.get(DATE)+ " " + map.get(TIME), map.get(LOCATION)+" "+ map.get(MESSAGE));
			return compDecompMap;
		}
		return null;
	}

	/**
	 * Check all information about ignoring obsolete output
	 * @param map
	 * @return
	 */
	private Map<String, String> checkObsoleteOutput(Map<String,String> map){
		if (map.get(MESSAGE).contains(IGNORING)){
			ignoreMap.put(map.get(DATE) + " " + map.get(TIME), map.get(LOCATION)+ " "+map.get(MESSAGE));
			return ignoreMap;
		}
		return null;
	}

	/**
	 *  Fetch the Tasklog URL and return it as a String: re-used from HiveMRTaskLogProcessor.java
	 */
	//	public static String fetchLog(MRTaskAttemptInfo attemptInfo) {
	public static String fetchLog(String inputString) {
		String retVal = null; 
		assert(inputString != null);
		URL taskAttemptLogUrl = null; 
		try {
			/*			urlString = "http://" + attemptInfo.getTaskTracker().getHostName() 
					+ ":" + attemptInfo.getTaskTracker().getPort() +
					"/tasklog?attemptid=" + attemptInfo.getExecId();*/
			//System.out.println("URL = " + urlString);
			String urlString = "http://inw-644.rfiserve.net:50060/tasklog?attemptid=" + inputString + "&all=true";
			taskAttemptLogUrl = new URL(urlString);
		} catch (MalformedURLException e) {
			/*			LOG.error("MalformedURLException while fetching URL: " + inputString);
			LOG.error("Error: ", e);*/
			System.err.println("MalformedURLException while fetching URL: " + inputString);
			return retVal; 
		}
		assert(taskAttemptLogUrl != null); 
		//System.err.println("taskAttemptLogUrl: " + taskAttemptLogUrl);

		InputStream in;
		BufferedReader reader;
		try {

			URLConnection c = taskAttemptLogUrl.openConnection();
			c.setConnectTimeout(TASKLOG_FETCH_CONNECTION_TIMEOUT_MILLIS);
			c.setReadTimeout(TASKLOG_FETCH_READ_TIMEOUT_MILLIS);

			//            System.out.println("printing the header title here: " + c.getHeaderField("title"));
			in = c.getInputStream();		
			//			in = taskAttemptLogUrl.openStream(); // short hand for openConnection().getInputStream
			reader = new BufferedReader(new InputStreamReader(in));
			String inputLine;

			while ((inputLine = HtmlQuoting.unquoteHtmlChars(reader.readLine())) != null) {
				System.out.println(inputLine);
			}
			//			retVal = HtmlQuoting.unquoteHtmlChars(IOUtils.toString(in));  // returns all text from webpage once
			IOUtils.closeQuietly(in); 
		} catch (Exception e) {
			LOG.error("Exception while reading from URL: " + inputString);
			LOG.error("Error: ", e);
		}

		return retVal; 

	} // public String fetchLog(MRTaskAttemptInfo attemptInfo) {

	/**
	 * To String for timeSpanList created for testing
	 * @param list
	 * @return
	 */
	protected String toString(List<String[]> list) {
		String output = "";
		String newLine = System.getProperty("line.separator");
		for (String[] array : list){
			String feedback = "line number is " + array[0] + " that took "
					+ array[1] + " milisecond to run: " + array[2];
			output =  output + newLine + feedback;
		}
		return output;
	}

	/**
	 * Getters and setters created for the purpose of testing
	 * @return
	 */
	public List<String[]> getTimeSpanList() {
		return timeSpanList;
	}

	public Map<String, String> getLineStructureMap(){
		return lineStructureMap;
	}

	public String getCompressionFormat() {
		return compressionFormat;
	}

	public String getLogSoFar(){
		return logSoFar;
	}

	public List<HashMap<String, String>> getMemoryList(){
		return memoryList;
	}

	public Map<Integer, String> getExceptionMap(){
		return exceptionMap;
	}

	public int getMaxMemory(){
		List<Integer> sizeList = new ArrayList<Integer>();
		for(HashMap<String, String> map: getMemoryList()){
			sizeList.add(Integer.parseInt(map.get("Memory")));
		}
		return Collections.max(sizeList);
	}
	
	public Map<String, Phase> getPhaseMap(){
		return phaseMap;
	}
	
	public Map<String, SignalDoctor> getDoctorMap(){
		return doctorMap;
	}

	@Override
	public void annotate(String input) {
		// TODO Auto-generated method stub

	}

	@Override
	public boolean saveLocal(String input) {
		// TODO Auto-generated method stub
		return false;
	}


	/*
	 *//**
	 * Run check on each line to extract information about spill
	 * @param lineStructureMap
	 *//*
	private void checkSpill(Map<String, String> map) {

		Boolean flag = false;
		String length = "length"; // target string
		String spill = "Finished spill"; // target string
		List<Integer> lengthList = new ArrayList<Integer>(), 
				recordList = new ArrayList<Integer>();
		Long spillTime = (long) 0;
		List<String> timeList = new ArrayList<String>();

		String message = map.get(MESSAGE);
		if (message.contains(length)){
			int spillLength = (int) ParseUtils.extractNumber(message).get(2);
			lengthList.add(spillLength);
			timeList.add(map.get(TIME) + " " + map.get(DATE));
			flag = true;
		}
		if (message.contains(spill)){
			int spillRecord  = (int) ParseUtils.extractNumber(message).get(0);
			recordList.add(spillRecord);
			timeList.add(map.get(TIME) + " " + map.get(DATE));
			flag = true;
		}

		if (flag){
			// Calculate total time on spilling
			for (int i = 0; i<= timeList.size(); i+=2){
				Long diff = ParseUtils.getTime(timeList.get(i+1)) - ParseUtils.getTime(timeList.get(i));
				spillTime += diff;
			}

			SpillPhase sp = new SpillPhase(Collections.max(lengthList),
					Collections.max(recordList),
					spillTime);
			phaseMap.put;
		}
	}*/







	// functionality for dividing the log, and annotate

	// time for in-memory merge, and time for waiting

}