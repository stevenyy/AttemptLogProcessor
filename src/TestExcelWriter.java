import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.sun.xml.internal.ws.policy.privateutil.PolicyUtils.IO;


public class TestExcelWriter {
	
	public static final String FOLDER = "/Users/Hadoop/Desktop/TestData/";
	public static final String ENTER_RETURN = System.getProperty("line.separator");
	
	public static final String SPILL = "/Users/Hadoop/Desktop/TestData/Spill/"; 
	public static final String RECORD_SPILL = SPILL + "RecordSpill.txt";
	public static final String TEST_SPILL = SPILL + "TestSpill.txt";
	
	public static final String EXP2SM = SPILL + "job_201405200258_413832_SM.txt";
	public static final String EXP3SM = SPILL + "job_201405200258_407819_SM.txt";
	public static final String EXP4SM = SPILL + "job_201405200258_425512_SM.txt";
	public static final String EXP5SM = SPILL + "job_201405200258_417851_SM.txt";
	public static final String EXP6SM = SPILL + "job_201405200258_420850_SM.txt";
	public static final String EXP7SM = SPILL + "job_201405200258_420817_SM.txt";
	
	private MRAttemptLogProcessor alp = new MRAttemptLogProcessor();
	private ExcelWriter ew = new ExcelWriter();

	@Before
	public void setUp() throws Exception {
		alp.readAndProcessLog(EXP2SM);
	}

	@After
	public void tearDown() throws Exception {

	}
	
	@Test
	public void testWriteSpillMergeStudy(){
//		alp.readAndProcessLog(RECORD_SPILL);
//		alp.readAndProcessLog(MAP_VERY_SHORT);
		SpillPhase sp =  (SpillPhase) alp.getPhaseMap().get("SpillPhase");
		SpillPhase sp2 =  (SpillPhase) alp.getPhasesResult().getSpillPhase();
		assertEquals(sp, sp2);

		System.out.println();
		System.out.println("Test SpillDoctor: the phase name is " + sp.getName());
		System.out.println("Test SpillDoctor: " + ENTER_RETURN +
				"SpillType is " + sp.getSpillType() + ENTER_RETURN+
				"Number of spill happened " + sp.getNumSpill()+  ENTER_RETURN + 
				"Total spill record " + sp.getTotalSpillRecord() +  ENTER_RETURN +
				"Total spill memory " + sp.getTotalSpillMemory() +  ENTER_RETURN +
				"Spilled time " + sp.getDuration() + ENTER_RETURN + 
				" ");
		
		MapMergePhase mmp =  (MapMergePhase) alp.getPhaseMap().get("MapMergePhase");

		System.out.println();
		System.out.println("Test MergeDoctor: the phase name is " + mmp.getName());
		System.out.println("Test SignalDoctor: " + ENTER_RETURN +
				mmp.getDuration() + " is the the merge time it takes" + ENTER_RETURN +
				mmp.getNumRedTasks() + " is the number of reduce tasks");
		
		List<String> idList = new ArrayList<String>();
		List<Integer> imSort = new ArrayList<Integer>();
		imSort.add(100);
		idList.add("testID");
		List<PhasesResult> list = new ArrayList<PhasesResult>();
		list.add(alp.getPhasesResult());
		ew.createSMTimeTable(list, idList,imSort);
	}

}
