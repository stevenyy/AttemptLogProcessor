

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Enumerates the different phases that MR tasks go through (eg. map, spill,
 * merge)
 * 
 * @author hero
 */
public enum MRTaskPhase {

	SHUFFLE{
/*		@Override
		public MRTaskPhase nextPhase(){
			
		}*/
	}, // Shuffle mrphases time in the reduce task
	SORT, // Merge mrphases time in the reduce task
	SETUP, // Setup mrphases time in the task
	READ, // Read mrphases time in the map task
	MAP, // Map mrphases time in the map task
	REDUCE, // Reduce mrphases time in the reduce task
	COLLECT, // Collect mrphases time in the map task
	WRITE, // write mrphases time in the reduce task
	SPILL, // Spill mrphases time in the map task
	MERGE, // Merge mrphases time in the map task
	CLEANUP, // Cleanup mrphases time in the task
	TOTAL; // The task running time from direct measurement to ensure accuracy
	

	/**
	 * @return a name for the task mrphases
	 */
	public String getName() {

		switch (this) {
		case SHUFFLE:
			return "SHUFFLE";
		case SORT:
			return "MERGE";
		case SETUP:
			return "SETUP";
		case READ:
			return "READ";
		case MAP:
			return "MAP";
		case REDUCE:
			return "REDUCE";
		case COLLECT:
			return "COLLECT";
		case WRITE:
			return "WRITE";
		case CLEANUP:
			return "CLEANUP";
		case SPILL:
			return "SPILL";
		case MERGE:
			return "MERGE";
		case TOTAL:
			return "TASK TIME";
		default:
			return toString();
		}
	}

	
	/**
	 * @return a description for the task mrphases
	 */
	public String getDescription() {

		switch (this) {
		case SHUFFLE:
			return "Shuffle mrphases: Transferring map output data to reduce tasks, with decompression if needed";
		case SORT:
			return "Merge mrphases: Merging sorted map outputs";
		case SETUP:
			return "Setup mrphases: Executing the user-defined setup function";
		case READ:
			return "Read mrphases: Reading the job input data from the distributed filesystem";
		case MAP:
			return "Map mrphases: Executing the user-defined map function";
		case REDUCE:
			return "Reduce mrphases: Executing the user-defined reduce function";
		case COLLECT:
			return "Collect mrphases: Partitioning and serializing the map output data to buffer before spilling";
		case WRITE:
			return "Write mrphases: Writing the job output data to the distributed filesystem";
		case CLEANUP:
			return "Cleanup mrphases: Executing the user-defined task cleanup function";
		case SPILL:
			return "Spill mrphases: Sorting, combining, compressing, and writing map output data to local disk";
		case MERGE:
			return "Merge mrphases: Merging sorted spill files";
		default:
			return toString();
		}
	}
	
    protected static Logger logger = LoggerFactory.getLogger(MRTaskPhase.class); // edited Steve

    /**
     * TODO: Implement State Transition Model incorporating ENUM: Steve
     * Returns the next state of the state machine depending on the current state and the content of lines around the line
     * that is currently being parsed.
     *
     * @param // something that can return the place of cursor, or which line is currently being read
     * @return the next state of the state machine.
     */
    
//	public abstract MRTaskPhase nextPhase();
}

