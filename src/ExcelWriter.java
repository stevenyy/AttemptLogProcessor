import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.poi.hssf.usermodel.HSSFSheet;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;


/**
 * Class used for creating, updating and writing excel sheet
 * Used Apache POI	  
 * @author Steve Siyang Wang
 */
public class ExcelWriter {
	// Members
	private HSSFWorkbook workbook; // The main workbook, which later becomes the excel file 
	private HSSFSheet sheet;// The work sheet
	private Map<String, Object[]> data; // default data map

	// Constant Strings
	private static final String NAME_DEFAULT = "Time Sheet"; 
	private static final String FOLDER = "";
	private static final String ADDRESS = "/Users/Hadoop/Desktop/TestData/Auto/";

	public ExcelWriter(){
		initialize();
	}

	/**
	 * Initialization, setting the target folder and initial parameters 
	 */
	private void initialize() {

		workbook = new HSSFWorkbook();
		//		sheet = workbook.createSheet(NAME_DEFAULT);
		data = new HashMap<String, Object[]>();
	}

	public void createSMTimeTable(List<PhasesResult> list, List<String> idList, List<Integer> ioSortMB){

		//		System.out.println("Testing if the phasesResult is identical " + list.get(0).equals(list.get(1)));

		Map<String, Object[]> spillMergeMap = new HashMap<String, Object[]>();
		HSSFSheet smStudy = makeNewSheet("Spill Merge Study");

		Object[] title = new Object[]{"Job_ID", "TaskAttempt_ID", "IO.SORT.MB", 
				"Total# Spill",	"# of KV Spills", "# of Meta Spills", 
				"Spilled Memory", "Spilled Record", 
				"Total Spill Time(mili)", "Total Spill Time(min)", 
				"Avg Data Size per Spill", "Avg Time per Spill", 
				"# of Reduce Tasks", "Ttl MergeTime(mili)", "Ttl MergeTime (min)", "Spill&Merge in Total(mili)"
		}; // size 17
		spillMergeMap.put("0", title);

		for (int i = 0; i<list.size(); i++){
			addEntry(list.get(i), spillMergeMap, idList.get(i), ioSortMB.get(i));
		}
		convertToTable(smStudy, spillMergeMap);
		saveWorkBook(workbook, "SpillMergeStudy.xls");
	}

	/*	private void makeMap(PhasesResult phasesResult, Map<String, Object[]> map){

	}*/
	private Map<String, Object[]> addEntry(PhasesResult phasesResult, Map<String, Object[]> data, String jobId, Integer integer) {

		SpillPhase sp = phasesResult.getSpillPhase();
		MapMergePhase mmp = phasesResult.getmMergePhase();
		InfoPhase ip = phasesResult.getInfoPhase();

		System.out.println("EW.write test ID same " + ip.getJobID() + " compared with that passed in " + jobId);
		long kv = sp.getSpillType().contains("KV") ? sp.getNumSpill():0 ;
		if (sp.getNumSpill()!=0){
			Object[] res = new Object[]{jobId, ip.getAttemptID(), integer, 
					sp.getNumSpill(), kv, sp.getNumSpill()-kv,
					sp.getTotalSpillMemory(), sp.getTotalSpillRecord(),
					sp.getDuration(), (double) sp.getDuration()/1000/60, 
					(double) sp.getTotalSpillMemory()/sp.getNumSpill(), (double) sp.getDuration()/sp.getNumSpill(), 
					mmp.getNumRedTasks(), mmp.getDuration(), (double) mmp.getDuration()/1000/60, sp.getDuration()+mmp.getDuration()}; 
			//TODO: improve this automatic casting

			data.put(Integer.toString(data.size() + 1), res);
		}
		return data;
	}

	/**
	 * Convert the DataMap to a table, writing each cell by cell by using loop
	 * @param smStudy
	 * @param dataMap
	 */
	private void convertToTable(HSSFSheet smStudy, Map<String, Object[]> dataMap) {
		//Extract this as a helper method
		try{
			int rowCounter = 0;
			for (String key : dataMap.keySet()) {
				Row row = smStudy.createRow(rowCounter++);
				Object [] objArr = dataMap.get(key);
				int colCounter = 0;
				for (Object obj : objArr) {
					Cell cell = row.createCell(colCounter++);
					if(obj instanceof Boolean)
						cell.setCellValue((Boolean)obj);
					else if (obj instanceof Long)
						cell.setCellValue((String) obj.toString()); 
					else if(obj instanceof String)
						cell.setCellValue((String) obj);
					else if(obj instanceof Double)
						cell.setCellValue((Double)obj);
					else if(obj instanceof Integer)
						cell.setCellValue((String) obj.toString());
				}
			}
			//			for(Object[] array: dataMap.values()){
			//				System.out.println(Arrays.toString(array));	
			//			}

		}
		catch (Throwable T){
			T.printStackTrace();
			System.err.println("The casting of cell value failed. Check the object array casting again");
		}
	}

	/**
	 * Write the file out
	 * @param workbook
	 * @param name
	 */
	private void saveWorkBook(HSSFWorkbook workbook, String name) {
		try {
			FileOutputStream out = 
					new FileOutputStream(new File(ADDRESS + name));
			workbook.write(out);
			out.close();
			System.out.println("Excel written successfully");

		} catch (FileNotFoundException e) {
			// Should not reach here
			e.printStackTrace();
			System.err.println("Save process failed. Target not found");
		} catch (IOException e) {
			// Should not reach here...
			e.printStackTrace();
			System.err.println("Save process failed. Consider checking EW.save method");
		}
	}

/*	//TODO: Other general multi-purpose write
	public void writeAndSave(PhasesResult phasesResult){

		addEntry(phasesResult, data, null);
		// other writeXXStudy here

	}*/

	//TODO:
	public void update(PhasesResult phasesResult){

	}

	// Add new sheet
	public HSSFSheet makeNewSheet(String name){
		return workbook.createSheet(name);
	}




}
