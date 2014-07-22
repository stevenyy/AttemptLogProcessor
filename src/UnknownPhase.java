import com.sun.org.apache.bcel.internal.classfile.Unknown;

/**
 * Unknown dummy phase for place holder so that doctor would not create null
 * phase, and thus causing NPE
 * @author Steve Siyang Wang
 */
public class UnknownPhase extends AbstractPhase{

    private long internalId; // The internal id for the object
    protected int hash; // The hash value for this object
    private String inputLog; // the most recent String of input that the phase read in

    private long duration;
    private String name = "UnknownPhase";
    private int dummyNum; // dummy number corresponds to the number in normal task phase

    public UnknownPhase(){
        super();
        initialize();
    }
    public UnknownPhase(String name){
        super();
        this.name = name;
        initialize();
    }

    public UnknownPhase(Long time, String name, int num){
        super();
        this.name = name;
        this.duration = time;
        this.dummyNum = -1;
    }

    private void initialize(){
        duration = (long) 0;
        dummyNum = -1;

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
}
