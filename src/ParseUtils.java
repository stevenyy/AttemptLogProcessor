

import java.io.IOException;
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

//import jdk.nashorn.internal.runtime.Context.ThrowErrorManager;


import org.apache.commons.io.IOUtils;


public class ParseUtils {

	// CONSTANTS
	public static final String WARN = "WARN";
	public static final String INFO = "INFO";
	public static final String ERROR = "ERROR";
    public static final String FATAL = "FATAL";
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
	public static final String DOT_TXT = ".txt"; 
	private static final String DOT_XML = ".xml";
	
	public static final Pattern JOB_PATTERN = Pattern
			.compile("(job_[0-9]+_[0-9]+)");
	public static final Pattern MAPATTEMPT_PATTERN = Pattern
			.compile("(attempt_[0-9]+_[0-9]+_m_[0-9]+_[0-9]+)");
	public static final Pattern REDUCEATTEMPT_PATTERN = Pattern
			.compile("(attempt_[0-9]+_[0-9]+_r_[0-9]+_[0-9]+)");
    public static final Pattern LINE_PATTERN = Pattern.
            compile("^(\\d{4}-\\d{2}-\\d{2}) (\\d{2}:\\d{2}:\\d{2},\\d{3}) (\\S+) ((\\[.*])?(\\s)?\\S+)(.+?)");
    public static final Pattern DATE_PATTERN = Pattern.
            compile("^((\\d{4}-\\d{2}-\\d{2}) (\\d{2}:\\d{2}:\\d{2},\\d{3}))(.*?)");

	
	
	public static final String START = "Start"; // Used for regex of bufStart, or KVStart
	public static final String END = "End";
	public static final String CAP = "Cap"; // Used for regex of bufVoid, or length
	
	//Data Connection Timeouts
	// See: http://eventuallyconsistent.net/2011/08/02/working-with-urlconnection-and-timeouts/
	public static final int TASKLOG_FETCH_CONNECTION_TIMEOUT_MILLIS = 5000; // 5 seconds
	public static final int TASKLOG_FETCH_READ_TIMEOUT_MILLIS = 10000; // 10 seconds
	
	
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
	
/*// returns the Integer array version. Consider updating to this in the future refactoring
	// !!! uncomplete
	public static int[] extractNumberArray(String input){
		List<Integer> numberList = new ArrayList<Integer>();
		Pattern numRegex = Pattern.compile("(\\d+)"); // The '\\d' for digit and '+' for one or more
		Matcher m = numRegex.matcher(input);
		
		while (m.find()){
			numberList.add(Integer.parseInt(m.group()));
		}
		for (Integer i: numberList){
			
		}
	}*/

	
	
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
     * Given any String, extract the time information and returns that
     * in Long data format.
     * Used for calculating time difference
     * @param Any String
     * @return Long representation of the date
     */
    public static Long getTime(String line){
        Date d = null;
        String dayAndTime = "none";
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss,SSS");
        try{
            Matcher matcher = DATE_PATTERN.matcher(line);
            matcher.matches();
            dayAndTime = matcher.group(1); // no 0 becuz we just need date time info, instead of the entire pattern

            //Finished pattern matching. Start converting the format
            d = format.parse(dayAndTime);
//            System.out.println("ParseUtils.getTime: Printing the date and time : " + dayAndTime);
            return d.getTime();
        } catch (Throwable T){
            //should not reach here...
            System.err.println("PU.getTime failed. Check the string passed in "
                    + "or check if the group is wrong");
            System.err.println("The dayAndTime it tried to convert to Long is " + dayAndTime);
            System.err.println("The line is " + line);
            T.printStackTrace();
            return null;
        }
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
	
	/**
	 * Returns a word or phrase before a particular pattern 
	 * @param s
	 * @return
	 */
	public static String getWordBefore(String line, String s){
		try{
			String[] split = line.split(s);
			String[] truncatedArray = split[0].split(ParseUtils.SPACE);
			return truncatedArray[truncatedArray.length - 1];

		}catch(Throwable t){
			t.printStackTrace();
			System.err.println("getWordBefore failed, the lind here is " + line + ENTER_RETURN +
					"the target split is " + s);
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
            Matcher matcher = LINE_PATTERN.matcher(line);
            if (matcher.matches()){
                map.put(DATE, matcher.group(1));
                map.put(TIME, matcher.group(2));
                map.put(MESSAGE_TYPE, matcher.group(3));
                map.put(LOCATION, matcher.group(4));
                // should not reach here...
                String message = matcher.group(matcher.groupCount());
                if (message.isEmpty() || message==null){
                    message = "empty";
                    map.put(MESSAGE, message);
                }else{
                    map.put(MESSAGE, message);
                }
            }
        } catch (Throwable T){
            T.printStackTrace();
            System.err.println("ExtractInfo failed Check Log line structure");
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
