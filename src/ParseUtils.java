

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.io.IOUtils;


public class ParseUtils {

	// CONSTANTS
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
	public static final String SPACE = "\\s+";
	
	
	/**
	 * Util class with static methods to facilitate the LogParser and improves re-usability
	 * @auther Steve Siyang Wang
	 */
	public ParseUtils(){
		// maybe unnecessary 
	}
	
	/**
	 * Returns the generic list of numbers, whether it is long, or int or double
	 * given a input string 
	 * NOTE: Need to cast from STRING to specific data type when using. 
	 * @param intput
	 * @return 
	 */
	public static List<String> extractNumber(String input){
		List<String> numberList = new ArrayList<String>();
		Pattern numRegex = Pattern.compile("(\\d+)"); // The '\\d' for digit and '+' for one or more
		Matcher m = numRegex.matcher(input);
		
		while (m.find()){
			numberList.add(m.group());
		}
		return numberList;

	}
	
	/**
	 * Return a particular line of log given the string log and integer line number
	 * @param log
	 * @param lineNum
	 * @return String line
	 */
	public static String getLine(String log, int lineNum){
		try {
			List<String> lines = IOUtils.readLines(new StringReader(log));
			return lines.get(lineNum - 1); // subtract one to make 0 the first index
		} catch (IOException e) {
			// Should not reach here... 
			e.printStackTrace();
		}
		return null;		
	}
	
	/**
	 * 
	 * Take in input string line, and return Map of structured info
	 * Every line is structured as below: "DATE TIME MESSAGE_TYPE LOCATION MESSAGE"
	 * @param Variable argument method, default first argument is the line
	 * @return Map<String, String> Structure
	 */
	public static Map<String, String> extractInfo(Object ... args){
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
	 * Given a line of log, returns the time in Long data format.
	 * Used for calculating time difference
	 * @param line
	 * @return Long representation of the date 
	 */
	public static Long getTime(String line){
		Date d = null;
		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss,SSS");
		Map<String, String> map = new HashMap<String, String>();
		String dayAndTime;
		if (line.split(SPACE).length > 3){
			map = extractInfo(line);
			dayAndTime = map.get(ParseUtils.DATE) + " " + map.get(ParseUtils.TIME);}
		else{
			dayAndTime = line;
		}
//		System.out.println("ParseUtils.getTime: Printing the date and time : " + dayAndTime);
		try {
			d = format.parse(dayAndTime);
		} catch (ParseException e) {
			System.err.println("The dayAndTime it tried to convert to Long is " + dayAndTime);
			e.printStackTrace();
		}

		return d.getTime();
	}
	
	/**
	 * return true if the line matches regex, and is in need of skipping 
	 * @param line
	 * @param lineCounter
	 * @return
	 */
	public static boolean skipLine(String line, int lineCounter) {
		Pattern skipRegex = Pattern.compile("(\\s*)(<)");
//		Pattern skipRegex = Pattern.compile("(\\s*)(<)(\\D+)(>)");
		Pattern exceptionRegex = Pattern.compile("(Exception)"); // 
		Pattern exceptionLocationRegex = Pattern.compile("(\\t)(at)");
		Pattern dateRegex = Pattern.compile("(\\d{4})-(\\d{2})-(\\d{2})(\\s{1})(\\d{2}):(\\d{2}):(\\d{2}),(\\d{3})");
		Matcher sm = skipRegex.matcher(line);
		Matcher em = exceptionRegex.matcher(line);
		Matcher elm = exceptionLocationRegex.matcher(line);
		Matcher dm = dateRegex.matcher(line);
		if (sm.find() && !dm.find()){
			return true;
		}
		if (em.find() || elm.find()){
			// Do something here
//			checkException(line, lineCounter);
			return true;
		}
		if (line.isEmpty()){
			return true;
		}
		return false;
	}
	
	/*	*//**
	 * Returns the date in long format given a line of log input
	 * @param line
	 * @return
	 *//*
	public static Long extractDate(String line){
		Pattern dateRegex = Pattern.compile("(\\d+)-(\\d+)-(\\d+)" + SPACE + "(\\d+):(\\d+):(\\d+),(\\d+)"); // The '\\d' for digit and '+' for one or more
		Matcher m = dateRegex.matcher(line);
		
		String date =  m.group();
	}*/

}
