
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
/**
 * Class representation of Spill Phase
 * @author Steve Siyang Wang
 */

//TODO: enable setLog functionality

public class SpillPhase extends AbstractPhase{
	
    private long internalId; // The internal id for the object
    protected int hash; // The hash value for this object
    private String inputLog; // the most recent String of input that the phase read in	
	
    private int spillMemory;
    private int spillRecord;
    private int[] dataBuffer; // The memory buffer array in kB: {80%, full}
    private int[] recordBuffer; // The record buffer in # or records: {80%, full}
    private String spillType; // The cause of spill, buffer full or record full
	private int numSpill;
	private long spillTime;
	private List<String[]> timeSpanList; // Map of big time-spans
	private String myLog;
	private String name;
	
	private List<HashMap<String, Integer>> bufList; //Map of buffer information 
	private List<HashMap<String, Integer>> kvList;//Map of kv information
	
	private List<Integer> memoryList, recordList;
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
		spillMemory = 0;
		spillRecord = 0;
		
		spillTime = (long) 0;
		numSpill = 0;
		dataBuffer = new int[2];
		recordBuffer = new int[2];
		timeSpanList = null;
		spillType = "No Spill Occured";
		myLog = null;
		bufList = new ArrayList<HashMap<String, Integer>>();
		kvList = new ArrayList<HashMap<String, Integer>>();
		memoryList = new ArrayList<Integer>();
		recordList = new ArrayList<Integer>();
	}
	
/*	public SpillPhase(int spillLength, int spillRecord, Long spillTime, String name){
		super();
		this.name = name;
		this.spillLength = spillLength;
		this.spillRecord = spillRecord;
		this.spillTime = spillTime;
	}*/
	
	public void update(List<Integer> memoryList, List<Integer> recordList,
			int numSpill, Long time) {
		this.memoryList = memoryList;
		this.recordList = recordList;
		this.numSpill = numSpill;
		this.spillTime = time;
		getTotalSpillMemory();
		getTotalSpillRecord();
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
	public int getNumSpill() {
		return numSpill;
	}
	
	public long getSpillTime() {
		return spillTime;
	}

	public void setSpillTime(long spillTime) {
		this.spillTime = spillTime;
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
		if (spillType.equals("record"))
			this.spillType = "Meta-Spill (record full = true)";
		if (spillType.equals("buffer"))
			this.spillType = "KV-Spill (buffer full = true)"; 
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

	public void setNumSpill(int numSpill) {
		this.numSpill = numSpill;
	}

	public List<Integer> getMemoryList() {
		return memoryList;
	}
	
	public int getTotalSpillMemory(){
//		System.out.println("Printing from SP: getTotalSpillMemory called");
//		System.out.println("Printing from SP: size of the memoryList " + memoryList.size());
		for (Integer i: memoryList){
			spillMemory += i.intValue();
		}
		return spillMemory;
	}
	
	public int getTotalSpillRecord(){
		for (Integer i: recordList){
			spillRecord += i.intValue();
		}
		return spillRecord;
	}

	public void setMemoryList(List<Integer> memoryList) {
		this.memoryList = memoryList;
	}

	public List<Integer> getRecordList() {
		return recordList;
	}

	public void setRecordList(List<Integer> recordList) {
		this.recordList = recordList;
	}

	@Override
	public String getName(){
		return name;
	}



}
