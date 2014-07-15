
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import edu.duke.starfish.profile.profileinfo.execution.mrtaskattempts.MRTaskAttemptInfo;

/**
 * TODO: replace all if with reflections
 * SignalDoctor that checks on each line to extract information about spill
 * @author Steve Siyang Wang
 */
public class SpillDoctor implements SignalDoctor{

	private String name = null;
	private MRTaskAttemptInfo attemptInfo = null;
	private int lineNum;
	private String line;
	private List<Integer> memoryList = new ArrayList<Integer>(), 
			numSpillList = new ArrayList<Integer>(), 
			recordList = new ArrayList<Integer>();
	private Long spillTime = (long) 0;
	private List<String> timeList = new ArrayList<String>();

	private String log;
	private int startNum, endNum;
	
	private SpillPhase sp;

	public SpillDoctor(){
		sp = new SpillPhase("SpillPhase");
	}

	public SpillDoctor(String name) {
		this.name= name;
		sp = new SpillPhase("SpillPhase");
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
		if (!skipLine(line, lineNum)){
			check(ParseUtils.extractInfo(line));	
		}
	}

	@Override
	public void check(String line){
		check(ParseUtils.extractInfo(line));
	}

	@Override
	public void check(Map<String, String> map){
		// Passing in lines that should not be skipped
		Boolean flag = false;
		String length = "length"; // target String:
		String finish = "Finished spill"; // target string:
		String full = "full = true"; // Target: cause of spill
		String bufStart = "bufStart";
		String dataBuffer = "data buffer = ";
		String recordBuffer = "record buffer = ";
		

		String message = map.get(ParseUtils.MESSAGE);
		List<String> list = ParseUtils.extractNumber(message); // TODO: can break potentially
		if (message.contains(full)){
			sp.setSpillType(ParseUtils.getWordBefore(message, full));
			// TODO: IMPROVE THIS, might have multiple full mode 
			timeList.add(map.get(ParseUtils.DATE)  + " " + map.get(ParseUtils.TIME));
		}
		if (message.contains(bufStart)){
			sp.updateBufList(list);
			memoryList.add(calculateDiffSize(list));
		}
		if (message.contains(length)){
			//			System.out.println("Printing from SpillDoc.check: the spill length is " + ParseUtils.extractNumber(message).get(2));
//			int spillLength = Integer.parseInt(ParseUtils.extractNumber(message).get(2));
			sp.updateKvList(list);
			recordList.add(calculateDiffSize(list));
			flag = true;
		}
		if (message.contains(finish)){
			int spillRecord  = Integer.parseInt(list.get(0));
			numSpillList.add(spillRecord);
			timeList.add(map.get(ParseUtils.DATE)  + " " + map.get(ParseUtils.TIME));
			flag = true;
		}
		if (message.contains(dataBuffer))
			sp.setDataBuffer(list);
		if (message.contains(recordBuffer))
			sp.setRecordBuffer(list);

	}

	/**
	 * Calculate the size of spill, both in terms of actual memory and record size
	 * @param list
	 * @return int difference
	 */
	private int calculateDiffSize(List<String> list) {
		int start = Integer.parseInt(list.get(0)); 
		int end = Integer.parseInt(list.get(1));
		if (end > start) {
			return (end - start);
		}
		else{
			return (end + Integer.parseInt(list.get(2)) - start);
		}
	}

	@Override
	public SpillPhase createPhase() {
//		System.out.println("createPhase in SpillDoctor called");
		
		try{
			calculateTime();
			//		System.out.println("debugging createPhase and spill time is " + spillTime);
			sp.update(
					memoryList,
					recordList,
					Collections.max(numSpillList),
					spillTime);
			return sp;
		} catch (Throwable T){
			System.err.println("SpillDoctor.createPhase failed, with possible reason "
					+ "that log parsing incomplete" + ParseUtils.ENTER_RETURN + "Check if lengthList, recordList, and SpillTime parameter exists");
			System.err.println();
			T.printStackTrace();
		}
		System.out.println(ParseUtils.ENTER_RETURN+ "SpillPhase created with incomplete fields");
		return sp;
	}

	/**
	 * Calculate total time on spilling
	 */
	private void calculateTime() {
		//		System.out.println("debugging calculateTime");
		int size = timeList.size();
		if (size%2 != 0){
			size = size - 1;
		}
		for (int i = 0; i< size; i+=2){
			//			System.out.println("printing the timeList from calculateTime " + timeList.get(i+1));
			//			System.out.println("printing the timeList from calculateTime " + timeList.get(i));
			Long diff = ParseUtils.getTime(timeList.get(i+1)) - ParseUtils.getTime(timeList.get(i));
			spillTime += diff;
		}

	}

	@Override
	public MRTaskAttemptInfo getAttemptInfo() {
		return attemptInfo; 
	}

	@Override
	public boolean reset() {
		this.attemptInfo = null;
		return true;
	}

	@Override
	public boolean open(MRTaskAttemptInfo attemptInfo) {
		this.attemptInfo = attemptInfo;
		return true;
	}

	@Override
	public boolean close() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public String getName() {
		if (name == null){
			System.out.println("SpillDoctor: you have not set the name of spillDoctor, thus "
					+ "returning null");	
			return null;
		}
		else{
		return name;}
	}

}
