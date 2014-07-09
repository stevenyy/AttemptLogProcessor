
/**
 *
 * Abstract class that represents the physical phase in an attempt or a task
 *
 * @author Steve Siyang Wang
 * Created on 6/30/14.
 */
public abstract class AbstractPhase {

    private long internalId; // The internal id for the object
    protected int hash; // The hash value for this object
    private String inputLog; // the most recent String of input that the phase read in
    private String name;
    /**
     * The default constructor
     */
    public AbstractPhase(){
        internalId = -1;
        hash = -1;
        inputLog = null;
        start();
    }

    /**
     * The start of a phase. This will be called in the constructor
     */
    public void start(){

    }

    public void next(){

    }

    public void end(){

    }
    
    public String getName(){
    	return name;
    }
    
    public void setName(String name){
    	this.name = name;
    }

    /**
     * Read data from a TaskAttemptLog
     */
    public void read(){

    }



}
