
public class MapMergePhase extends AbstractPhase{
	
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
	public MapMergePhase(String name){
		super();
		this.name = name;
		initialize();
		
	}
	
	public MapMergePhase(Long time, String name){
		super();
		this.mergeTime = time;
		this.name = name;
	}
	
	private void initialize(){
		mergeTime = (long) 0;
		
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
	
	@Override
	public String getName(){
		return name;
	}

}
