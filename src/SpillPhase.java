
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
	
    private int spillLength;
	private int spillRecord;
	private long spillTime;
	private List<String[]> timeSpanList; // Map of big time-spans
	private String myLog;
	private String name;
	
	/**
	 * TODO: SpillPhase, consider changing all the fields to final to restrict later modification 
	 * @param string 
	 */
	
	public SpillPhase(String string){
		super();
		name = string;
	}
	
	public SpillPhase(int spillLength, int spillRecord, Long spillTime, String name){
		super();
		this.spillLength = spillLength;
		this.spillRecord = spillRecord;
		this.spillTime = spillTime;
		this.name = name;
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
	
	@Override
	public String getName(){
		return name;
	}

}
