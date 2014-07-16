
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
	
    private long spillMemory;
    private long spillRecord;
    private long[] dataBuffer; // The memory buffer array in kB: {80%, full}
    private long[] recordBuffer; // The record buffer in # or records: {80%, full}
    private String spillType; // The cause of spill, buffer full or record full
	private long numSpill;
	private long duration;
	private List<String[]> timeSpanList; // Map of big time-spans
	private String myLog;
	private String name;
	
	private List<HashMap<String, Long>> bufList; //Map of buffer information 
	private List<HashMap<String, Long>> kvList;//Map of kv information
	
	private List<Long> memoryList;
	private List<Long> recordList;
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
		
		duration = (long) 0;
		numSpill = 0;
		dataBuffer = new long[2];
		recordBuffer = new long[2];
		timeSpanList = null;
		spillType = "No Spill Occured";
		myLog = null;
		bufList = new ArrayList<HashMap<String, Long>>();
		kvList = new ArrayList<HashMap<String, Long>>();
		memoryList = new ArrayList<Long>();
		recordList = new ArrayList<Long>();
	}
	
/*	public SpillPhase(int spillLength, int spillRecord, Long spillTime, String name){
		super();
		this.name = name;
		this.spillLength = spillLength;
		this.spillRecord = spillRecord;
		this.spillTime = spillTime;
	}*/
	
	public void update(List<Long> memoryList, List<Long> recordList,
			int numSpill, Long time) {
		this.memoryList = memoryList;
		this.recordList = recordList;
		this.numSpill = numSpill;
		this.duration = time;
		calculateTotalSpillMemory();
		calculateTotalSpillRecord();
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
	public long getNumSpill() {
		return numSpill;
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

	public long[] getDataBuffer() {
		if (dataBuffer == null)
			System.err.println("DataBuffer is not populated. Spill did not happen");
		return dataBuffer;
	}
	
	public long[] getRecordBuffer() {
		if (recordBuffer == null)
			System.err.println("RecordBuffer is not populated. Spill did not happen");
		return recordBuffer;
	}

	public void setRecordBuffer(List<String> list) {
		// Better Use try catch and check size of list 
		this.recordBuffer = new long[]{Integer.parseInt(list.get(0)), Integer.parseInt(list.get(1))};
	}
	
	public void setDataBuffer(List<String> list) {
		// Better to Use Try Catch and Check size of List
		this.dataBuffer = new long[]{Integer.parseInt(list.get(0)), Integer.parseInt(list.get(1))};
	}
	
	public List<HashMap<String, Long>> getBufList(){
		if (bufList == null)
			System.err.println("bufList not populated");
		return bufList;
	}
	
	public List<HashMap<String, Long>> getKvList(){
		if (kvList == null)
			System.err.println("kvList not populated");
		return kvList;
	}
	
	public void updateBufList(List<String> list){
		try{
			HashMap<String, Long> bufMap = new HashMap<String, Long>();
			bufMap.put(ParseUtils.START, Long.parseLong(list.get(0)));
			bufMap.put(ParseUtils.END, Long.parseLong(list.get(1)));
			bufMap.put(ParseUtils.CAP, Long.parseLong(list.get(2)));
			bufList.add(bufMap);} 
		catch(Throwable t){
			t.printStackTrace();
			System.err.println("The lenght of list " + Arrays.toString(list.toArray()) + "does not match 3 "
					+ ParseUtils.ENTER_RETURN + " consider checking size ");
		}
	}
	
	public void updateKvList(List<String> list){
		try{
			HashMap<String, Long> kvMap = new HashMap<String, Long>();
			kvMap.put(ParseUtils.START, Long.parseLong(list.get(0)));
			kvMap.put(ParseUtils.END, Long.parseLong(list.get(1)));
			kvMap.put(ParseUtils.CAP, Long.parseLong(list.get(2)));
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

	public List<Long> getMemoryList() {
		return memoryList;
	}
	
	private long calculateTotalSpillMemory(){
		if (spillMemory == 0){
			for (Long i: memoryList){
//				System.out.println("SP.CalTotalSM: Each element in memory list is " + i.longValue());
				spillMemory += i.longValue();
//				System.out.println("SP: the spillMemory here is " + spillMemory);
//				spillMemory = spillMemory + i;
			}
		}
		
		return spillMemory;

	}
	
	public long getTotalSpillMemory(){
		return spillMemory;
	}
	
	public long getTotalSpillRecord(){
		return spillRecord;
	}
	
	private long calculateTotalSpillRecord(){
		if (spillRecord == 0){
			for (Long i: recordList){
				spillRecord += i.longValue();
			}
		}
		return spillRecord;
	}

	public void setMemoryList(List<Long> memoryList) {
		this.memoryList = memoryList;
	}

	public List<Long> getRecordList() {
		return recordList;
	}

	public void setRecordList(List<Long> recordList) {
		this.recordList = recordList;
	}

	@Override
	public String getName(){
		return name;
	}
	
	@Override
	public long getDuration() {
		return duration;
	}

	public void setDuration(long spillTime) {
		this.duration = spillTime;
	}



}
