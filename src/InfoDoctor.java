
import java.awt.image.MemoryImageSource;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.sun.javafx.scene.EnteredExitedHandler;

import edu.duke.starfish.profile.profileinfo.execution.mrtaskattempts.MRTaskAttemptInfo;

/**
 * Info Doctor that checks on each line to extract information about spill
 * @author Steve Siyang Wang
 */
public class InfoDoctor implements SignalDoctor{

	public static final Long MAX_INTERVAL = (long) 30000; // The maxi time interval between two lines should be half a minute  
	private String name = null;
	private MRTaskAttemptInfo attemptInfo = null;
	private int lineNum = 0;
	private int previousNum = 0;
	private int previousExceptionNum = 0;
	private String line = null;
	private String previousLine = null;
	private InfoPhase ip;
	private String exceptionLog;

	private List<Integer> lengthList = new ArrayList<Integer>(), 
			recordList = new ArrayList<Integer>();
	private Long spillTime = (long) 0;
	private List<String> timeList = new ArrayList<String>();

	private String log;
	private int startNum, endNum;

	private List<String[]> timeSpanList; // Map of big time-spans
//	private List<HashMap<String, String>> memoryList; // Map of memory-usage


	public InfoDoctor(){
		// Think whether to get rid of this
		ip = new InfoPhase("InfoPhase");
	}

	public InfoDoctor(String name) {
		this.name= name;
		ip = new InfoPhase("InfoPhase");
	}

	@Override 
	public void check(Map<String, String> map, String line, int lineNum){
		// Need to check if this previous/ current framework works
		this.previousLine = this.line;
		this.previousNum = this.lineNum;
		this.line = line;
		this.lineNum = lineNum;
		check(map);	
	}

	@Override
	public void check(String line, int lineNum) {
		this.lineNum = lineNum;
		check(ParseUtils.extractInfo(line));
	}

	@Override
	public void check(String line){
		check(ParseUtils.extractInfo(line));
	}

	// WORK ON HERE
	@Override
	public void check(Map<String, String> map){

		if (!skipLine(line, lineNum)){

			checkTimeSpan(line, previousLine, lineNum);
			checkCompressionLibrary(map.get(ParseUtils.MESSAGE));
			checkMemoryUsage(map, line, lineNum);
			checkTag(map);
			checkCodecPool(map);
			checkObsoleteOutput(map);

		}
	}

	@Override
	public InfoPhase createPhase() {
		//		System.out.println("debugging createPhase and spill time is " + spillTime);
		return ip;
	}

	private void calculateTime() {
		//		System.out.println("debugging calculateTime");
		// Do something
	}

	/**
	 * Check line against the skip regex: true if the line is to skip
	 * @param line
	 * @return boolean 
	 */
	@Override 
	public boolean skipLine(String line, int lineNum) {
		Pattern skipRegex = Pattern.compile("(\\s*)(<)"); // User-specify the regex at which the pattern is matching
		Pattern exceptionRegex = Pattern.compile("(Exception)"); // 
		Pattern exceptionLocationRegex = Pattern.compile("(\\t)(at)");
		Matcher sm = skipRegex.matcher(line);
		Matcher em = exceptionRegex.matcher(line);
		Matcher elm = exceptionLocationRegex.matcher(line);
		if (sm.find()){
			return true;
		}
		if (em.find() || elm.find()){
			// Do something here
			logException(line, previousExceptionNum, lineNum);
			previousExceptionNum = lineNum;
			return true;

		}
		if (line.isEmpty()){
			return true;
		}
		return false;
	}

	/**
	 * Keep track of all exceptions happened, and store them into List<Strings> 
	 * @param line, previous line num, and current line num.
	 */
	private void logException(String line, int previous, int current){

		if (current - previous > 2){ // The gap in line number indicates a new Exception excerpt
			if (exceptionLog!=null){
				ip.updateExceptionMap(current, exceptionLog);
				exceptionLog = null; // clears the exceptionLog once it is saved
			}
			else{exceptionLog += line + ParseUtils.ENTER_RETURN;}
		}
		exceptionLog += line + ParseUtils.ENTER_RETURN;
	}

	/**
	 * Check all information about ignoring obsolete output
	 * @param map
	 * @return
	 */
	private void checkObsoleteOutput(Map<String,String> map){
		if (map.get(ParseUtils.MESSAGE).contains(ParseUtils.IGNORING)){
			ip.updateObsoleteOutputMap(map.get(ParseUtils.DATE) + " " + map.get(ParseUtils.TIME), map.get(ParseUtils.LOCATION)+ " "+map.get(ParseUtils.MESSAGE));
		}
	}

	/**
	 * Gather all information about the compressor and decompressor
	 * @param map
	 * @return
	 */
	private void checkCodecPool(Map<String, String> map){
		if (map.get(ParseUtils.LOCATION).contains(ParseUtils.CODEC_POOL)){
			ip.updateCodecMap(map.get(ParseUtils.DATE)+ " " + map.get(ParseUtils.TIME), map.get(ParseUtils.LOCATION)+" "+ map.get(ParseUtils.MESSAGE));
		}
	}

	/**
	 * Gather all WARN and ERROR tagged log, and directly update the InfoPhase object, to avoid storing data 
	 * in InfoDoctor to save data setting overhead. 
	 * @param
	 * @return warnMap
	 */
	private void checkTag(Map<String, String> map){
		switch (map.get(ParseUtils.MESSAGE_TYPE)){
		// TODO: need test if warnMap and errorMap is updated interanlly

		case ParseUtils.WARN:  
			ip.updateWarnMap(lineNum, map.get(ParseUtils.DATE) + " " + map.get(ParseUtils.TIME) + " "
					+  map.get(ParseUtils.LOCATION) + " " + map.get(ParseUtils.MESSAGE) );
		case ParseUtils.ERROR: 
			ip.updateErrorMap(lineNum, map.get(ParseUtils.DATE) + " " + map.get(ParseUtils.TIME) + " "
					+  map.get(ParseUtils.LOCATION) + " " + map.get(ParseUtils.MESSAGE) );
		}

	}

	/**
	 * Check the memory-usage information reflected in the log being parsed, 
	 * and then save that information to InfoPhase class object
	 * NEED to cast String to number type when using
	 * @param List of Maps
	 * @return
	 */
	private void checkMemoryUsage(Map<String, String> map, String line, int lineNum) {

		String message = map.get(ParseUtils.MESSAGE);
		//		System.out.println("CheckMemoryUsage message is " + message );
		if (message.contains("used memory")){
			HashMap<String, String> memoryInfo = new HashMap<String, String>();
			// Key stored in the sequence of: Line, Rows, Memory, Message

			List<String> numList = ParseUtils.extractNumber(message);
			memoryInfo.put("Line", Integer.toString(lineNum));
			memoryInfo.put("Rows", numList.get(0));
			memoryInfo.put("Memory", numList.get(1));
			memoryInfo.put("Message", line);
			// For debug:
			//			System.out.println("Print from checkMemoryUsage : ");
			//			System.out.println("CheckMemoryUsage: the size of mr is " + mr.size());
			
			ip.updateMemoryList(memoryInfo);
		}
	}

	/**
	 * TODO: make this more robust
	 * Check the compression format
	 * @param message
	 * @return String compressionFormat
	 */
	private String checkCompressionLibrary(String message){
		String compressLib = null;
		if (message.contains(ParseUtils.LIBRARY)){
			String[] splitArray = message.split(ParseUtils.SPACE);
			compressLib = splitArray[splitArray.length-2];
		}
		ip.setCompressLib(compressLib);
		return compressLib;
	}

	/**
	 * 
	 * Check the interval between two line in log, and return those
	 * @param current
	 * @param logCounter
	 * @return List of String Array with format line#, duration, message
	 */
	private void checkTimeSpan(String current, String previous, int lineNum){
		if (previous != null && !skipLine(previous, lineNum)){ 
			// Make sure previous is not null or un-parsable
			long diff = ParseUtils.getTime(current) - ParseUtils.getTime(previous);
			if (diff > MAX_INTERVAL){
				// Construct a feedback String array with format: line#, duration, message
				String[] feedback = {Integer.toString(lineNum), Long.toString(diff), current};
				ip.updateTimeSpanList(feedback);
			}
		}
	}


	@Override
	public MRTaskAttemptInfo getAttemptInfo() {
		return attemptInfo; 
	}

	@Override
	public boolean reset() {
		this.attemptInfo = null;
		return true;
	}

	@Override
	public boolean open(MRTaskAttemptInfo attemptInfo) {
		this.attemptInfo = attemptInfo;
		return true;
	}

	@Override
	public boolean close() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public String getName() {
		if (name == null){
			System.out.println("SpillDoctor: you have not set the name of spillDoctor, thus "
					+ "returning null");
			return null;}
		else{return name;}
	}

}
