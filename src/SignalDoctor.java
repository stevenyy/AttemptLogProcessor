
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import edu.duke.starfish.profile.profileinfo.execution.mrtaskattempts.MRTaskAttemptInfo;

public interface SignalDoctor{

	String id = null;
	MRTaskAttemptInfo attemptInfo = null;
	String name = null;

	public MRTaskAttemptInfo getAttemptInfo();

	public boolean reset();

	public boolean open(MRTaskAttemptInfo attemptInfo);

	public boolean close();
	
	public abstract String getName();
	
	public abstract void check(Map<String, String> map, String line, int lineNum);

	public abstract void check(String line, int lineNum);
	
	public abstract void check(String line);
	
	public abstract void check(Map<String, String> map);
	
	public abstract Phase createPhase();
	
	/**
	 * Check line against the skip regex: true if the line is to skip
	 * @param line
	 * @return boolean 
	 */
	public default boolean skipLine(String line, int lineCounter) {
		Pattern skipRegex = Pattern.compile("(\\s*)(<)"); // User-specify the regex at which the pattern is matching
		Pattern exceptionRegex = Pattern.compile("(Exception)"); // 
		Pattern exceptionLocationRegex = Pattern.compile("(\\t)(at)");
		Matcher sm = skipRegex.matcher(line);
		Matcher em = exceptionRegex.matcher(line);
		Matcher elm = exceptionLocationRegex.matcher(line);
		if (sm.find()){
			return true;
		}
		if (em.find() || elm.find()){
			// Do something here
//			checkException(line, lineCounter);
			return true;
		}
		if (line.isEmpty()){
			return true;
		}
		return false;
	}
	
//	public abstract boolean process(LogLineBuffer buffer, long maxLineNum);

} // public class SignalExtractor {

/*for reference:
	
	private String id = null;
	private MRTaskAttemptInfo attemptInfo = null;

	public SignalDoctor(){
		
	}
	
	public SignalDoctor(String id) {
		this.id = id;
	}

	public String getId() {
		return id;
	}

	public MRTaskAttemptInfo getAttemptInfo() {
		return attemptInfo; 
	}

	public boolean reset() {
		this.attemptInfo = null;
		return true;
	}

	public boolean open(MRTaskAttemptInfo attemptInfo) {
		this.attemptInfo = attemptInfo;
		return true;
	}*/