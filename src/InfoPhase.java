import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * InfoPhase, a phase that stores all the information obtained from parsing.  
 */
public class InfoPhase extends AbstractPhase {

    private long internalId; // The internal id for the object
    protected int hash; // The hash value for this object
    private String inputLog; // The attempt log as a String
	private String name; // Name of this InfoPhase
	
	private String jobID;
	private String attemptID;
	
    private String compressLib;
	private List<String[]> timeSpanList; // Map of big time-spans
	private List<HashMap<String, String>> memoryList; // Map that details Memory Usages
	
	private Map<Integer, String> exceptionMap; // Map of exceptions occurred
	private Map<Integer, String> warnMap; // Map of WARN related message
	private Map<Integer, String> errorMap; // Map of ERROR related message
	private Map<String, String> codecMap; // Map of CodecPool
	private Map<String, String> obsoleteOutputMap; // Map of obsolete output that's ignored during the log
	
	//TODO: better to store Some Overall Informations
	//Job-ID, attemptID, totalTime, etc
	
	public InfoPhase(){
		super();
		initialize();
	}
	
	public InfoPhase(String name){
		super();
		initialize();
//		this.compressLib= compressLib;
//		this.spillRecord = spillRecord;
//		this.spillTime = spillTime;
		this.name = name;
	}
	
	/**
	 * Called during the constructor to initialize the map to be used later
	 */
	private void initialize(){
		
		name = " ";
		jobID = " ";
		mapAttemptID = " ";
		reduceAttemptID = " ";
		
		compressLib = " ";
		timeSpanList = new ArrayList<String[]>();
		memoryList = new ArrayList<HashMap<String, String>>();
		exceptionMap = new HashMap<Integer, String>();
		warnMap = new HashMap<Integer, String>();
		errorMap = new HashMap<Integer, String>();
		codecMap = new HashMap<String, String>();
		obsoleteOutputMap = new HashMap<String, String>();
		
	}
	
	public void update(String log){
		
	}
	
	@Override 
	public void start(){
		
	}
	
	@Override
	public void next(){
		
	}
	
	@Override 
	public void end(){
		
	}
	
	
	/**
	 * Getters and setters for accessing and setting field values
	 * @return
	 */

	public List<String[]> getTimeSpanList() {
		return timeSpanList;
	}

	public void setTimeSpanList(List<String[]> timeSpanList) {
		this.timeSpanList = timeSpanList;
	}
	
	public void updateTimeSpanList(String[] array){
		timeSpanList.add(array);
	}
	
	@Override
	public String getName(){
		return name;
	}

	public String getCompressLib() {
		return compressLib;
	}

	public void setCompressLib(String compressLib) {
		this.compressLib = compressLib;
	}

	public Map<String, String> getCodecMap() {
		return codecMap;
	}

	public void setCodecMap(Map<String, String> codecMap) {
		this.codecMap = codecMap;
	}
	
	public void updateCodecMap(String s1, String s2){
		codecMap.put(s1, s2);
	}

	public Map<Integer, String> getExceptionMap() {
		return exceptionMap;
	}

	public void setExceptionMap(Map<Integer, String> exceptionMap) {
		this.exceptionMap = exceptionMap;
	}
	
	public void updateExceptionMap(int lineNum, String out){
		exceptionMap.put(lineNum, out);
	}

	public Map<Integer, String> getWarnMap() {
		return warnMap;
	}

	public void setWarnMap(Map<Integer, String> warnMap) {
		this.warnMap = warnMap;
	}
	
	public void updateWarnMap(int lineNum, String info){
		warnMap.put(lineNum, info);
	}

	public Map<Integer, String> getErrorMap() {
		return errorMap;
	}

	public void setErrorMap(Map<Integer, String> errorMap) {
		this.errorMap = errorMap;
	}
	
	public void updateErrorMap(int lineNum, String info){
		errorMap.put(lineNum, info);
	}

	public List<HashMap<String, String>> getMemoryList() {
		return memoryList;
	}

	public void setMemoryList(List<HashMap<String, String>> memoryList) {
		this.memoryList = memoryList;
	}
	
	public void updateMemoryList(HashMap<String, String> map){
		memoryList.add(map);
	}

	public String getInputLog() {
		return inputLog;
	}

	public void setInputLog(String inputLog) {
		this.inputLog = inputLog;
	}

	public Map<String, String> getObsoleteOutputMap() {
		return obsoleteOutputMap;
	}

	public void setObsoleteOutputMap(Map<String, String> obsoleteOutputMap) {
		this.obsoleteOutputMap = obsoleteOutputMap;
	}
	
	public void updateObsoleteOutputMap(String s1, String s2){
		obsoleteOutputMap.put(s1, s2);
	}

	public void setJobID(String id) {
		this.jobID = id;
	}
	
	public String getJobID(){
		return jobID;
	}

	public String getAttemptID() {
		return attemptID;
	}

	public void setAttemptID(String AttemptID) {
		this.attemptID = AttemptID;
	}


}
