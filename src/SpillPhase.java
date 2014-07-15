
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import com.sun.javafx.scene.EnteredExitedHandler;
/**
 * Class representation of Spill Phase
 * @author Steve Siyang Wang
 */

//TODO: enable setLog functionality

public class SpillPhase extends AbstractPhase{
	
    private long internalId; // The internal id for the object
    protected int hash; // The hash value for this object
    private String inputLog; // the most recent String of input that the phase read in	
	
    private int spillLength;
    private int[] dataBuffer; // The memory buffer array in kB: {80%, full}
    private int[] recordBuffer; // The record buffer in # or records: {80%, full}
    private String spillType; // The cause of spill, buffer full or record full
	private int spillRecord;
	private long spillTime;
	private List<String[]> timeSpanList; // Map of big time-spans
	private String myLog;
	private String name;
	
	private List<HashMap<String, Integer>> bufList; //Map of buffer information 
	private List<HashMap<String, Integer>> kvList;//Map of kv information
	
	/**
	 * TODO: SpillPhase, consider changing all the fields to final to restrict later modification 
	 * @param string 
	 */
	
	public SpillPhase(String string){
		super();
		this.name = string;
		initialize();
	}
	
	/**
	 * Invoked in the constructor to set all parameters
	 */
	private void initialize(){
		spillLength = 0;
		spillRecord = 0;
		spillTime = (long) 0;
		dataBuffer = new int[2];
		recordBuffer = new int[2];
		timeSpanList = null;
		spillType = "No Spill Occured";
		myLog = null;
		bufList = new ArrayList<HashMap<String, Integer>>();
		kvList = new ArrayList<HashMap<String, Integer>>();
	}
	
	public SpillPhase(int spillLength, int spillRecord, Long spillTime, String name){
		super();
		this.name = name;
		this.spillLength = spillLength;
		this.spillRecord = spillRecord;
		this.spillTime = spillTime;
	}
	
	public void update(Integer length, Integer rec, Long time){
		spillLength = length;
		spillRecord = rec;
		spillTime = time;
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
	public int getSpillLength() {
		return spillLength;
	}

	public void setSpillLength(int spillLength) {
		this.spillLength = spillLength;
	}
	
	public long getSpillTime() {
		return spillTime;
	}

	public void setSpillTime(long spillTime) {
		this.spillTime = spillTime;
	}
	
	public int getSpillRecord() {
		return spillRecord;
	}

	public void setSpillRecord(int spillRecord) {
		this.spillRecord = spillRecord;
	}

	public List<String[]> getTimeSpanList() {
		return timeSpanList;
	}

	public void setTimeSpanList(List<String[]> timeSpanList) {
		this.timeSpanList = timeSpanList;
	}

	public String getSpillType() {
		return spillType;
	}

	public void setSpillType(String spillType) {
		this.spillType = spillType;
	}

	public int[] getDataBuffer() {
		if (dataBuffer == null)
			System.err.println("DataBuffer is not populated. Spill did not happen");
		return dataBuffer;
	}
	
	public int[] getRecordBuffer() {
		if (recordBuffer == null)
			System.err.println("RecordBuffer is not populated. Spill did not happen");
		return recordBuffer;
	}

	public void setRecordBuffer(List<String> list) {
		// Better Use try catch and check size of list 
		this.recordBuffer = new int[]{Integer.parseInt(list.get(0)), Integer.parseInt(list.get(1))};
	}
	
	public void setDataBuffer(List<String> list) {
		// Better to Use Try Catch and Check size of List
		this.dataBuffer = new int[]{Integer.parseInt(list.get(0)), Integer.parseInt(list.get(1))};
	}
	
	public List<HashMap<String, Integer>> getBufList(){
		if (bufList == null)
			System.err.println("bufList not populated");
		return bufList;
	}
	
	public List<HashMap<String, Integer>> getKvList(){
		if (kvList == null)
			System.err.println("kvList not populated");
		return kvList;
	}
	
	public void updateBufList(List<String> list){
		try{
			HashMap<String, Integer> bufMap = new HashMap<String, Integer>();
			bufMap.put(ParseUtils.START, Integer.parseInt(list.get(0)));
			bufMap.put(ParseUtils.END, Integer.parseInt(list.get(1)));
			bufMap.put(ParseUtils.CAP, Integer.parseInt(list.get(2)));
			bufList.add(bufMap);} 
		catch(Throwable t){
			t.printStackTrace();
			System.err.println("The lenght of list " + Arrays.toString(list.toArray()) + "does not match 3 "
					+ ParseUtils.ENTER_RETURN + " consider checking size ");
		}
	}
	
	public void updateKvList(List<String> list){
		try{
			HashMap<String, Integer> kvMap = new HashMap<String, Integer>();
			kvMap.put(ParseUtils.START, Integer.parseInt(list.get(0)));
			kvMap.put(ParseUtils.END, Integer.parseInt(list.get(1)));
			kvMap.put(ParseUtils.CAP, Integer.parseInt(list.get(2)));
			kvList.add(kvMap);} 
		catch(Throwable t){
			t.printStackTrace();
			System.err.println("The lenght of list " + Arrays.toString(list.toArray()) + "does not match 3 "
					+ ParseUtils.ENTER_RETURN + " consider checking size ");
		}
	}

	@Override
	public String getName(){
		return name;
	}

}
