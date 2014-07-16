import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashMap;
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
	
	private Map<String, Object[]> data;
	
	// Constant Strings
	private static final String NAME_DEFAULT = "Time Sheet"; 
	private static final String FOLDER = "";
	private static final String ADDRESS = "";
	
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
	
	public void writeAndSave(PhasesResult phasesResult){
		
		writeSpillMergeStudy(phasesResult);
		// other writeXXStudy here
		
	}
	
	//TODO: improve this so that it can be called repeatedly
	private void writeSpillMergeStudy(PhasesResult phasesResult) {
		
		HSSFSheet smStudy = workbook.createSheet("Spill Merge Study");
		SpillPhase sp = phasesResult.getSpillPhase();
		MapMergePhase mmp = phasesResult.getmMergePhase();
		
		System.out.println("EW.write testing the size of data map " + data.size());
		
		Object[] title = new Object[]{"Job_ID", "TaskAttempt_ID", "IO.SORT.MB", 
				"Total# Spill",	"# of KV Spills", "# of Meta Spills", 
				"Spilled Memory", "Spilled Record", 
				"Total Spill Time(mili)", "Total Spill Time(min)", 
				"Avg Data Size per Spill", "Avg Time per Spill", 
				"# of Reduce Tasks", "Ttl MergeTime(mili)", "Ttl MergeTime (min)", "Spill&Merge in Total(mili)"
		}; // size 17
		
		long kv = sp.getSpillType().contains("KV") ? sp.getNumSpill():0 ;
		Object[] res = new Object[]{"ID", "aID", "IO", 
				sp.getNumSpill(), kv, sp.getNumSpill()-kv,
				sp.getTotalSpillMemory(), sp.getTotalSpillRecord(),
				sp.getDuration(), (double) sp.getDuration()/1000/60, 
				(double) sp.getTotalSpillMemory()/sp.getNumSpill(), (double) sp.getDuration()/sp.getNumSpill(), 
				mmp.getNumRedTasks(), mmp.getDuration(), (double) mmp.getDuration()/1000/60, sp.getDuration()+mmp.getDuration()}; 
		//TODO: improve this automatic casting
		
		System.out.println("EW: it proceeds to here");
		data.put(Integer.toString(data.size() + 1), title);
		data.put(Integer.toString(data.size() + 1), res);
		
		//Extract this as a helper method
		try{
			int rowCounter = 0;
			for (String key : data.keySet()) {
				Row row = smStudy.createRow(rowCounter++);
				Object [] objArr = data.get(key);
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
		}
		catch (Throwable T){
			T.printStackTrace();
			System.err.println("The casting of cell value failed. Check the object array casting again");
		}

		// Extract Helper Method. File Write Out
		try {
		    FileOutputStream out = 
		            new FileOutputStream(new File("/Users/Hadoop/Desktop/TestData/Auto/SpillMergeStudy.xls"));
		    workbook.write(out);
		    out.close();
		    System.out.println("Excel written successfully..");
		     
		} catch (FileNotFoundException e) {
		    e.printStackTrace();
		} catch (IOException e) {
		    e.printStackTrace();
		}

		/*		System.out.println();
		System.out.println("Test SpillDoctor: the phase name is " + sp.getName());
		System.out.println("Test SpillDoctor: " + ENTER_RETURN +
				"SpillType is " + sp.getSpillType() + ENTER_RETURN+
				"Number of spill happened " + sp.getNumSpill()+  ENTER_RETURN + 
				"Total spill record " + sp.getTotalSpillRecord() +  ENTER_RETURN +
				"Total spill memory " + sp.getTotalSpillMemory() +  ENTER_RETURN +
				"Spilled time " + sp.getDuration() + ENTER_RETURN + 
				" ");*/
		
	}

	public void update(PhasesResult phasesResult){
		
	}
	
	// Add new sheet
	public HSSFSheet makeNewSheet(String name){
		return workbook.createSheet(name);
	}
	
	
	
	

}
