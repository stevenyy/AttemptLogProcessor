package test;
import LogParser;

import java.io.*;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Pattern;

/**
 * TODO: create generic Interface for a generic parser to parse the log.
 *
 * @author Steve Siyang Wang
 * Created on 6/30/14.
 */
public class LogParser2 {

    private String lastLog = null; // The last log it read
    // private List<Check> checkList; // List of Doctor
    private Map<String, String> lineStructureMap; // Map of line message structure
    private Map<String, String> warnMap; // Map of WARN related message
    private Map<String, String> compDecompMap; // Map of Compressor and De-compressor related message
    private Map<String, String> ignoreMap; // Map of ignoring obsolete input
    private Map<Integer, String[]> timeSpanMap = new HashMap<Integer, String[]>();; // Map of big time-spans 
    /*?? Why ArrayList of Arrays would not work in Java?*/
    private String compressionFormat;
    private int logCounter; // Count the line number of the log

    // CONSTANTS
    public static final String WARN = "WARN";
    public static final String INFO = "INFO";
    public static final String CODEC_POOL = "CodecPool";
    public static final String IGNORING = "ignoring";
    public static final String LIBRARY = "library";

    public static final String DATE = "Date";
    public static final String TIME = "Time";
    public static final String MESSAGE_TYPE = "MessageType";
    public static final String LOCATION = "Location";
    public static final String MESSAGE = "Message";
    private static final String SPACE = "\\s+";

    private static final long MAX_INTERVAL = 60000; // the interval between log time in millisecond, default 1 minute

    private static final String DOT_XML = ".xml";
    private static final Pattern NAME_PATTERN = Pattern
            .compile(".*(job_[0-9]+_[0-9]+).*");

    /**
     * Static method that allow direct invoking
     * TODO: Used for small file only, need to improve it to memory buffer
     * @return boolean that indicate whether succeeded or not
     */

    public String readAndProcessLog(String path){
        // Read and parse from the local file directory with small file size
        try{

            InputStream in = new FileInputStream(new File(path));
            BufferedReader reader = new BufferedReader(new InputStreamReader(in));
            StringBuilder out = new StringBuilder();
            String line;
            String previous = null;
            logCounter = 0;
            while ((line = reader.readLine()) != null) {
                logCounter++;
                out.append(line + System.getProperty("line.separator"));
                extractInfo(line);
                
                // Diagnose at the line basis
                checkTimeSpan(line,previous, logCounter);
                checkCompressionLibrary(line);

                checkWarn(lineStructureMap);
                checkCodecPool(lineStructureMap);
                checkIgnore(lineStructureMap);


                /* Alternative: register each doctor interface, and loop through
                for (Check c: checkList){
                    c.diagnose(line);
                }

                Alternative:
                StringWriter writer = new StringWriter();
                IOUtils.copy(inputStream, writer, encoding);
                String theString = writer.toString();
                */
                previous = line;
            }

            lastLog = out.toString();
//            System.out.println(lastLog);   //Prints the string content read from input stream
            reader.close();

//            Charset encoding = Charset.defaultCharset();
//            byte[] encoded = Files.readAllBytes(Paths.get(path));
//            lastLog = new String(encoded, encoding);

        } catch (FileNotFoundException e) {
            e.printStackTrace();
            System.out.println("File not found. Re-check the URL address");
        } catch (IOException e) {
            e.printStackTrace();
        }
        return lastLog;

        // Read and parse from HTTP request
        // See HiveMRTTaskLogProcessor for detail in HTTP Connect

    }

    /**
     * Take in input string line, and return Map of structured info
     * Every line is structured as below: "DATE TIME MESSAGE_TYPE LOCATION MESSAGE"
     * @param line
     * @return Map<String, String> Structure
     */
    private Map<String, String> extractInfo(String line){
        lineStructureMap = new HashMap<String, String>();
        String[] tokens = line.split(SPACE);
        lineStructureMap.put(DATE, tokens[0]);
        lineStructureMap.put(TIME, tokens[1]);
//        System.out.println("the second token is " + tokens[2]);
        lineStructureMap.put(MESSAGE_TYPE, tokens[2]);
        lineStructureMap.put(LOCATION, tokens[3]);
        String message = line.split(tokens[3])[1];
        lineStructureMap.put(MESSAGE, message);
        return lineStructureMap;
    }

    /**
     * Check the interval between two line in log, and return those
     * @param current
     * @param logCounter
     * @return
     */
    private Map<Integer, String[]> checkTimeSpan(String current, String previous, int logCounter){
        String currentDateTime = extractInfo(current).get(DATE) + " " + extractInfo(current).get(TIME);
        String previousDateTime = null; Date d1 = null, d2 = null;
        // establish a date format
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss,SSS");
        if (previous != null) {
            previousDateTime = extractInfo(previous).get(DATE) + " " + extractInfo(previous).get(TIME);
//            System.out.println("the current state date and time is " + currentDateTime);
//            System.out.println("the previous state date and time is " + previousDateTime);
            try {
                d1 = format.parse(previousDateTime);
                d2 = format.parse(currentDateTime);
            } catch (ParseException e) {
                e.printStackTrace();
            }
            long diff = d2.getTime() - d1.getTime();
//            System.out.println("the difference in time being printed out is "+ diff);

            if (diff > MAX_INTERVAL){
                // Construct a feedback String array with format: line#, duration, message
                String[] feedback = new String[] {Long.toString(diff), current};
                System.out.println("printing the feedback arraylist here" + toString(feedback));

                timeSpanMap.put(logCounter, feedback);
                System.out.println("the size of the timeSpanList is here " + timeSpanMap.size());
                return timeSpanMap;
            }else
            	return null;
        }
        return null;
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
     * Gather all information in the log. Called Repeatedly.
     * @param
     * @return warnMap
     */
    private Map<String, String> checkWarn(Map<String, String> map){
        warnMap = new HashMap<String, String>();
        if (map.get(MESSAGE_TYPE).equals(WARN)){
            warnMap.put(map.get(DATE)+map.get(TIME), map.get(LOCATION)+map.get(MESSAGE));
            return warnMap;
        }
        return null;
    }

    /**
     * Gather all information about the compressor and decompressor
     * @param map
     * @return
     */
    private Map<String, String> checkCodecPool(Map<String, String> map){
        compDecompMap = new HashMap<String, String>();
        if (map.get(LOCATION).contains(CODEC_POOL)){
            compDecompMap.put(map.get(DATE)+map.get(TIME), map.get(LOCATION)+map.get(MESSAGE));
            return compDecompMap;
        }
        return null;
    }

    /**
     * Check all information about ignoring obsolete output
     * @param map
     * @return
     */
    private Map<String, String> checkIgnore(Map<String,String> map){
        ignoreMap = new HashMap<String, String>();
        if (map.get(MESSAGE).contains(IGNORING)){
            ignoreMap.put(map.get(DATE)+map.get(TIME), map.get(LOCATION)+map.get(MESSAGE));
            return ignoreMap;
        }
        return null;
    }
    
    public String getCompressionFormat(){
    	return compressionFormat;
    }
    
    public Map<Integer, String[]> getTimeSpanList(){
    	return timeSpanMap;
    }
    
    public static void main(String[] args){
        String REDUCE1 = "/Users/Hadoop/Desktop/TestData/attempt_201405200258_319232_r_000000_0_SlowSuccess.txt";
    	String test1 = "/Users/Hadoop/Desktop/TestData/ShortTest.txt";
    	String test2 = "/Users/Hadoop/Desktop/TestData/SuperShort.txt";
        LogParser lp = new LogParser();
        lp.readAndProcessLog(test2);
//        System.out.println(lp.getTimeSpanList().get(0)[2]);
        System.out.println(lp.toString(lp.getTimeSpanList()));
        System.out.println(lp.getCompressionFormat());
    }
    
    private String toString(Map<Integer, String[]> map){
    	String output = null;
    	System.out.println("here the list size is " + map.size());
    	for (int i = 0; i<map.size(); i++){
    		String feedback = "line number is " + map.get(i)[0] + " with time span of " 
    				+ map.get(i)[1] + " mili second, that says: " + map.get(i)[2];
//    		System.out.println(feedback);
    		output = output + feedback;
    	}
		return output;
    }
    
    private String toString(String[] input) {
		// TODO Auto-generated method stub
		String feedback = " Time span of " + input[0] + " mili second, that says: " + input[1];
		return feedback;
	}

    

    // functionality for dividing the log, and annotate

    // time for in-memory merge, and time for waiting





}