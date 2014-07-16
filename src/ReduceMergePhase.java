


public class ReduceMergePhase extends AbstractPhase {

	private long internalId; // The internal id for the object
	protected int hash; // The hash value for this object
	private String inputLog; // the most recent String of input that the phase read in	

	private long duration;
	private String name;

	/**
	 * ReduceMergePhase, the second phase among 4 in the reduce process. It merges the sorted fragments 
	 * from the different mappers to form the input to the reduce function.
	 * @author Steve Siyang Wang 
	 */
	public ReduceMergePhase(){
		super();
	}

	public ReduceMergePhase(Long time, String name){
		super();
		this.name = name;
		this.duration = time;
	}


	@Override
	public String getName(){
		return name;
	}
	
	@Override 
	public long getDuration(){
		return duration;
	}
	
	public void setDuration(long mergeTime){
		this.duration = mergeTime;
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


}
