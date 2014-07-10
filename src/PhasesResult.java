
public class PhasesResult {


	/*Member of the class, consider TODO: using ENUMS
	 * ***************/
	/*
	 * private ReadPhase readPhase;
	 * private MapPhase mapPhaes;
	 * private CollectPhase collectPhase;*/
	private SpillPhase spillPhase;
	private MapMergePhase mMergePhase;

	private ShufflePhase shufflePhase;
	private ReduceMergePhase rMergePhase;
	private ReducePhase reducePhase;
	private WritePhase writePhase;
	
	private InfoPhase infoPhase; // Phase object that stores general info about the task attempt log

	/** 
	 * A class object that stores all the phases created during a Log Parsing 
	 * session. It stores them as fields, enabling easier access and management
	 * It also stores some essential information about particular parsing session.
	 * 
	 * TODO: consider changing to ENUM for easier access to Phase instances
	 */
	public PhasesResult(){

	}

	public void registerPhase(String name, AbstractPhase phase){
		try{ // Uses switch case with string for better performance 
			switch (name){
			case "SpillPhase" : 
				spillPhase = (SpillPhase) phase;
				break;
			case "MapMergePhase" : 
				mMergePhase = (MapMergePhase) phase;
				break;
			case "ShufflePhase" : 
				shufflePhase = (ShufflePhase) phase;
				break;
			case "ReduceMergePhase" : 
				rMergePhase = (ReduceMergePhase) phase;
				break;
			case "ReducePhase" : 
				reducePhase = (ReducePhase) phase;
				break;
			case "WritePhase" : 
				writePhase = (WritePhase) phase;
				break;

			case "InfoPhase" : 
				infoPhase = (InfoPhase) phase;
				break;
			}
		}
		catch (Throwable T){
			// Should not reach here... 
			System.err.println("RegisterPhase: register to PhasesResult failed. Check type-casting");
			T.printStackTrace();
		}
	}
	
	
	/**
	 * Getters and setters for accessing all phases
	 * @return
	 */
	public SpillPhase getSpillPhase() {
		return spillPhase;
	}

	public void setSpillPhase(SpillPhase spillPhase) {
		this.spillPhase = spillPhase;
	}

	public MapMergePhase getmMergePhase() {
		return mMergePhase;
	}

	public void setmMergePhase(MapMergePhase mMergePhase) {
		this.mMergePhase = mMergePhase;
	}

	public ShufflePhase getShufflePhase() {
		return shufflePhase;
	}

	public void setShufflePhase(ShufflePhase shufflePhase) {
		this.shufflePhase = shufflePhase;
	}

	public ReduceMergePhase getrMergePhase() {
		return rMergePhase;
	}

	public void setrMergePhase(ReduceMergePhase rMergePhase) {
		this.rMergePhase = rMergePhase;
	}

	public ReducePhase getReducePhase() {
		return reducePhase;
	}

	public void setReducePhase(ReducePhase reducePhase) {
		this.reducePhase = reducePhase;
	}

	public WritePhase getWritePhase() {
		return writePhase;
	}

	public void setWritePhase(WritePhase writePhase) {
		this.writePhase = writePhase;
	}

	public InfoPhase getInfoPhase() {
		return infoPhase;
	}

	public void setInfoPhase(InfoPhase infoPhase) {
		this.infoPhase = infoPhase;
	}

}
