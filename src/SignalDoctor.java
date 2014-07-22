
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
	
	public abstract AbstractPhase createPhase();
	
	public abstract boolean skipLine(String line, int lineCounter);
	
/*	*//**
	 * Check line against the skip regex: true if the line is to skip
	 * @param line
	 * @return boolean 
	 *//*
	public default boolean skipLine(String line, int lineCounter) {
		try {
//			System.out.println("Print Signal Doctor: the line that stopped is " + line);
			Pattern skipRegex = Pattern.compile("(\\s*)(<)");
			Pattern exceptionRegex = Pattern.compile("(Exception)"); // 
			Pattern exceptionLocationRegex = Pattern.compile("(\\t)(at)");
			Pattern dateRegex = Pattern.compile("(\\d{4})-(\\d{2})-(\\d{2})(\\s{1})(\\d{2}):(\\d{2}):(\\d{2}),(\\d{3})");
			Matcher sm = skipRegex.matcher(line);
			Matcher em = exceptionRegex.matcher(line);
			Matcher elm = exceptionLocationRegex.matcher(line);
			Matcher dm = dateRegex.matcher(line);
			if (sm.find() && !dm.find()){
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
		} catch (Throwable T){
			T.printStackTrace();
			System.err.println("The line number is " + lineCounter);
			System.err.println("The line that caused halt is ：" + line);
		}
		return false;
	}*/
	
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