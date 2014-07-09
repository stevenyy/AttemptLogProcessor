
public class MapMergePhase extends Phase{
	
    private long internalId; // The internal id for the object
    protected int hash; // The hash value for this object
    private String inputLog; // the most recent String of input that the phase read in	
    
    private Long mergeTime;
    private String name;
	
    
	/**
	 * MapMergePhase, the last and fifth phase in the map process. It de-compresses the spills and merge those 
	 * with buffer and 
	 * @author Steve Siyang Wang 
	 */
	public MapMergePhase(){
		super();
	}
	
	public MapMergePhase(Long time, String name){
		super();
		this.mergeTime = time;
		this.name = name;
	}
	
	@Override
	public String getName(){
		return name;
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
	 * Getters for the sake of testing
	 */
	public Long getMergeTime(){
		return mergeTime;
	}

}
