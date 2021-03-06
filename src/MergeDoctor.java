import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import edu.duke.starfish.profile.profileinfo.execution.mrtaskattempts.MRTaskAttemptInfo;

/**
 * Merge SignalDoctor that checks on each line to extract information about Merge Phase,
 * including Merge Phase in both Map and Reduce 
 * @author Steve Siyang Wang
 */
public class MergeDoctor implements SignalDoctor{

	private String name = null;
	private MRTaskAttemptInfo attemptInfo = null;
	private int lineNum;
	private String line;

	private List<Integer> mergeRecordList = new ArrayList<Integer>();
	private List<String> timeList = new ArrayList<String>();
	private Long mergeTime = (long) 0;
	private int mNumRedTask = 0;

	private String log;
	private int startNum, endNum;

	private boolean mapMerge = false; // flag that indicates which merge phase this is, i.e. MergePhase during Map 
	private boolean reduceMerge = false; // ....., i.e. MergePhase during Reduce

	public MergeDoctor(){
	}

	public MergeDoctor(String name){
		this.name = name;
	}

	@Override 
	public void check(Map<String, String> map, String line, int lineNum){
		this.line = line;
		this.lineNum = lineNum;
		check(map);
	}

	@Override
	public void check(String line, int lineNum) {
		this.lineNum = lineNum;
		this.line = line;
		if (!skipLine(line, lineNum)){
			check(ParseUtils.extractInfo(line));	
		}		
	}

	@Override
	public void check(String line) {
		check(ParseUtils.extractInfo(line));		
	}

	@Override
	public void check(Map<String, String> map) {

		Boolean flag = false;

		String mapTask = "MapTask";
		String reduceTask = "ReduceTask";
		String merge = "Merging"; // target string
		String redTask = "numReduceTasks"; // target String

		String message = map.get(ParseUtils.MESSAGE);
		if (message.contains(merge)){

			//			System.out.println("Printing from MergeDoc.check: the mergeRecord is " + ParseUtils.extractNumber(message).get(0));
			int mergeRecord = Integer.parseInt(ParseUtils.extractNumber(message).get(0));
			mergeRecordList.add(mergeRecord);
			timeList.add(map.get(ParseUtils.DATE)  + " " + map.get(ParseUtils.TIME));
			flag = true;

		}
		if (message.contains(redTask)){
			mNumRedTask = Integer.parseInt(ParseUtils.extractNumber(message).get(0));
		}
		if(map.get(ParseUtils.LOCATION).contains(mapTask)) 
			mapMerge = true;
		if(map.get(ParseUtils.LOCATION).contains(reduceTask))
			reduceMerge = true;	
	}

	@Override
	public AbstractPhase createPhase() {
		//		System.out.println("MergeDoctor.createPhase called");
		try{
			calculateTime();
			if (mapMerge){
				MapMergePhase mmp = new MapMergePhase(mergeTime, "MapMergePhase", mNumRedTask);
				return mmp;
			}
			if (reduceMerge){
				ReduceMergePhase rmp = new ReduceMergePhase(mergeTime, "ReduceMergePhase");
				return rmp;
			}
			else{ // should not reach here unless broken log
				UnknownPhase up = new UnknownPhase(mergeTime, "UnknownPhase", -1);
				return up;
			}
		}catch (Throwable T){
			System.err.println("MergeDoctor.createPhase failed, with possible reason "
					+ "that log parsing incomplete");
			T.printStackTrace();
		}
		return null;
	}

	private void calculateTime(){
		//		System.out.println("printing the size of the timeList " + timeList.size());
		if (timeList.size()!=0){
			long init = ParseUtils.getTime(timeList.get(0));
			long end = ParseUtils.getTime(timeList.get(timeList.size() - 1));
			mergeTime = end - init;
		}
	}

	/**
	 * Check line against the skip regex: true if the line is to skip
	 * @param line
	 * @return boolean 
	 */
	@Override
	public boolean skipLine(String line, int lineCounter) {
		try {
			//				System.out.println("Print Signal Doctor: the line that stopped is " + line);
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
	}


	@Override
	public MRTaskAttemptInfo getAttemptInfo() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean reset() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean open(MRTaskAttemptInfo attemptInfo) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean close() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public String getName() {
		return name;
	}

}
