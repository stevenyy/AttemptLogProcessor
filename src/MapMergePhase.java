
public class MapMergePhase extends AbstractPhase{
	
    private long internalId; // The internal id for the object
    protected int hash; // The hash value for this object
    private String inputLog; // the most recent String of input that the phase read in	
    
    private long duration;
    private String name;
    private int numReduceTasks;
	
    
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
	
	public MapMergePhase(Long time, String name, int num){
		super();
		this.duration = time;
		this.name = name;
		this.numReduceTasks = num;
	}
	
	private void initialize(){
		duration = (long) 0;
		numReduceTasks = 0;
		
	}
	

	public void update(Long time, int num){
		this.duration = time;
		this.numReduceTasks = num;
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
	@Override
	public long getDuration(){
		return duration;
	}
	
	public void setDuration(long mergeTime){
		this.duration = mergeTime;
	}
	
	@Override
	public String getName(){
		return name;
	}
	
	public int getNumRedTasks(){
		return numReduceTasks;
	}

}
