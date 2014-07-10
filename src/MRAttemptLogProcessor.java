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

import com.sun.javafx.scene.EnteredExitedHandler;



/**
 * TODO: create generic Interface for a generic parser to parse the log.
 *
 * @author Steve Siyang Wang
 * Created on 6/30/14.
 */

public class MRAttemptLogProcessor implements LogAnnotator {

	private String logSoFar; // The last log it read
	
	private Map<String, String> lineStructureMap; // Structure of each line in log, updated per extractInfo call
	private Map<Integer, String> exceptionMap; // Map of exceptions occurred
	private List<HashMap<String, String>> memoryList; // Map of memory-usage
	private List<String[]> timeSpanList; // Map of big time-spans
	private String compressionFormat;
	private int lineNum; // Count the line number of the log
	private Map<String, AbstractPhase> phaseMap; // List of phases created
	private PhasesResult phasesResult; // Class that stores all the phases as field/instance, 

	
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
	private InfoDoctor id;
	private Map<String, SignalDoctor> doctorMap;

	/**
	 * Static method that allow direct invoking
	 * TODO: Used for small file only, need to improve it to memory buffer
	 * @return boolean that indicate whether succeeded or not
	 */

	public MRAttemptLogProcessor(){
		logSoFar = null;
		lineStructureMap = new HashMap<String, String>(); 
		exceptionMap = new HashMap<Integer, String>();
		memoryList = new ArrayList<HashMap<String, String>>();
		timeSpanList = new ArrayList<String[]>();
		phaseMap= new HashMap<String, AbstractPhase>();
		phasesResult = new PhasesResult();

		//Initializign SignalDoctors
		initializeDoctors();
	}

	private void initializeDoctors() {
		doctorMap= new HashMap<String, SignalDoctor>();
		sd = new SpillDoctor("SpillDoctor");
		md = new MergeDoctor("MergeDoctor");
		id = new InfoDoctor("InfoDoctor");

		doctorMap.put("SpillDoctor", sd);
		doctorMap.put("MergeDoctor", md);
		doctorMap.put("InfoDoctor", id);
	}

	//	public String readAndProcessLog(MRTaskAttemptInfo attemptInfo, String attemptID){
	public PhasesResult readAndProcessLog(String filePath){
		//		Map<String, String> lineStructureMap = new HashMap<String, String>();
		// Read and parse from the local file directory with small file size
		InputStream in;
		StringBuilder out = new StringBuilder();
		String line;
		String previous = null;
		lineNum = 1;

		try{
			in = new FileInputStream(new File(filePath));
			BufferedReader reader = new BufferedReader(new InputStreamReader(in));
			while ((line = reader.readLine()) != null) {
				out.append(line + ParseUtils.ENTER_RETURN);
				
//				lineStructureMap = ParseUtils.extractInfo(line, lineNum, logSoFar);
				//				System.out.println("The current line at which it stopped " + lineCounter);
				if (ParseUtils.skipLine(line, 0)){
					System.out.println("Printing the skipped line: " + line);
				}
				for (SignalDoctor doctor : doctorMap.values()) {
					doctor.check(line, lineNum);
//											System.out.println("printing from SignalDoc loop");
				}				
				previous = line;
				lineNum++;
			}
			// Ask SignalDoctors to create Phases
			for (SignalDoctor doctor : doctorMap.values()) {
				AbstractPhase p = doctor.createPhase();
				System.out.println("Checking the name " + p.getName());
				phaseMap.put(p.getName(), p);
				System.out.println("Checking the size of map " + phaseMap.size());
				phasesResult.registerPhase(p.getName(), p);
			}

			logSoFar = out.toString();
//			            System.out.println("Printing from main loop log so far is "+ logSoFar);   //Prints the string content read from input stream
			reader.close();

		} catch (FileNotFoundException e) {
			e.printStackTrace();
			System.out.println("File not found. Re-check the URL address");
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		return phasesResult;

		// Read and parse from HTTP request
		// See HiveMRTTaskLogProcessor for detail in HTTP Connect
	}

	/**	
	 * Return the line number as well as the wait time during the shuffle wait
	 * @param lineStructureMap2
	 * @return 
	 */

	private void checkWaitTime(Map<String, String> lineStructureMap) {
		// incomplete
		Long waitTime = (long) 0;
		return;
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
	
	public PhasesResult getPhasesResult(){
		return phasesResult;
	}
	
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

	public Map<String, AbstractPhase> getPhaseMap(){
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