
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.*;
import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertEquals;

/**
 * Test the AttemptLogProcessor
 *
 * @author Steve Siyang Wang
 */
public class TestAttemptLogProcessor{
	public static final String FOLDER = "/Users/Hadoop/Desktop/TestData/";
	public static final String REDUCE1 = FOLDER + "attempt_201405200258_319232_r_000000_0_SlowSuccess.txt";
	public static final String FAST_SUCCESS = FOLDER + "attempt_201405200258_221067_m_000000_0_FastSuccess.txt";
	public static final String SHORT_TEST = FOLDER+ "ShortTest.txt";
	public static final String SUPER_SHORT = FOLDER + "SuperShort.txt";
	public static final String LARGE_TEXT = FOLDER + "LargeText.txt";
	public static final String PROBLEM = FOLDER + "Problem.txt";
	public static final String ERROR = FOLDER + "Error.txt";
	public static final String VERY_LONG = FOLDER + "VeryLong.txt";
	public static final String LINE_RETURN = FOLDER + "LineReturn.txt";
	public static final String MEMORY_TEST = FOLDER + "MemoryTest.txt";
	public static final String MEMORY_TEST2 = FOLDER + "MemoryTest2.txt";
	public static final String MAP_SPILL = FOLDER + "MapSpill.txt";
	public static final String MAP_LONG = FOLDER + "MapLong.txt";
	public static final String MAP_SHORT = FOLDER + "MapShort.txt";
	public static final String MAP_VERY_SHORT = FOLDER + "MapVeryShort.txt";
	public static final String ENTER_RETURN = System.getProperty("line.separator");
	
	private MRAttemptLogProcessor alp= new MRAttemptLogProcessor();

	@Before
	public void setUp() throws Exception {
		//ToDo: get connection info from properties instead of hard-wiring
		//ProfileDBManager dao = new ProfileDBManager("org.postgresql.Driver", url);
		//dao = new ProfileDBManager("com.mysql.jdbc.Driver", jdbcUrl);
		//dao = ProfileDBManager.instance();
		alp.readAndProcessLog(MAP_LONG);
	}

	@After
	public void tearDown() throws Exception {

	}

//	@Test 
	public void testFetchLog(){
//		String url = "http://unravel.rfiserve.net/workflows/show_by_exec_id/20140624T171819Z-6913431618608607502";
//		String url = "http://inw-729.rfiserve.net:50060/tasklog?attemptid=attempt_201405200258_352939_m_000001_0&all=true";
		String url = "attempt_201405200258_368899_m_000006_0";
		
		System.out.println();
		System.out.println(alp.fetchLog(url));
	}
	
	
//	@Test
	public void testTimeSpan(){
	
//		lp.readAndProcessLog(ERROR);
//		lp.readAndProcessLog(PROBLEM);
//		lp.readAndProcessLog(ERROR);
		alp.readAndProcessLog(SHORT_TEST);
		
		List<String[]> timeSpanList = alp.getTimeSpanList();
		System.out.println();
		System.out.println("Printing Test TimeSpan");
		System.out.println(alp.toString(timeSpanList));
//		System.out.println(lp.getLogSoFar()); // Print the logSoFar that is obtained
	}
	
//	@Test
	public void testLineStructureMap(){
//		lp.readAndProcessLog(PROBLEM);
		alp.readAndProcessLog(ERROR);
//		lp.readAndProcessLog(SHORT_TEST);
		System.out.println();
		System.out.println(ENTER_RETURN + "Test extractInfo: The sample structure of the information is ");
		System.out.println(" The date is " + alp.getLineStructureMap().get("Date") + ENTER_RETURN +
				" the time is " + alp.getLineStructureMap().get("Time") + ENTER_RETURN +
				" the message type is " + alp.getLineStructureMap().get("MessageType") + ENTER_RETURN +
				" the message is " + alp.getLineStructureMap().get("Message"));
	}
	
//	@Test 
	public void testSkipLine(){
		String SKIP1 = "<MAP>Id =3";
		String SKIP2 = "              <Parent>Id = 1 null<Parent>";
		String SKIP3 = " <\\EX>";
		
/*		assertEquals(true, alp.checkSkipLine(SKIP1));
		assertEquals(true, alp.checkSkipLine(SKIP2));
		assertEquals(true, alp.checkSkipLine(SKIP3));*/
		System.out.println();
		System.out.println("Printing Test SkipLine: ");
		System.out.println("Skipped Strings are printed below: " + ENTER_RETURN + SKIP1 + ENTER_RETURN + SKIP2 
				+ ENTER_RETURN + SKIP3);
	}

//	@Test
	public void testZipLib(){
		alp.readAndProcessLog(SHORT_TEST);
		System.out.println();
		System.out.println("Printing Test ZipLib: ");
		System.out.println(ENTER_RETURN + alp.getCompressionFormat());
		assertEquals("native-zlib", alp.getCompressionFormat());
	}
	
//	@Test
	public void testCheckMemory(){
//		lp.readAndProcessLog(MEMORY_TEST);
//		lp.readAndProcessLog(MEMORY_TEST2);
		alp.readAndProcessLog(REDUCE1);
		System.out.println();
		System.out.println("Printing Test CheckMemory: ");
		for (HashMap<String, String> map: alp.getMemoryList()){
			System.out.println("Memory Use in #" + map.get("Line") + 
					" when ExecReducer processing: " + map.get("Rows") + " rows and " +   
					" Total Memory so far used: " + map.get("Memory"));
		}
	}

//	@Test 
	public void testMaxMemory(){
		alp.readAndProcessLog(REDUCE1);
		System.out.println();
		System.out.println("Printing Test MaxMemory: ");
		System.out.println(alp.getMaxMemory());
	}

	
//	@Test 
	public void testException(){
//		lp.readAndProcessLog(ERROR);
//		lp.readAndProcessLog(VERY_LONG);
		alp.readAndProcessLog(LINE_RETURN);
		System.out.println();
		System.out.println("Printing Test Exception: ");
		for (String message: alp.getExceptionMap().values()){
			System.out.println("The message at the exception is: " + message);
		}
	}
	
	@Test 
	public void testSpillDoctor(){
		
//		alp.readAndProcessLog(MAP_LONG);
		SpillPhase sp =  (SpillPhase) alp.getPhaseMap().get("SpillPhase");
		SpillPhase sp2 =  (SpillPhase) alp.getPhasesResult().getSpillPhase();
		
		assertEquals(sp, sp2);

		System.out.println();
		System.out.println("Test SpillDoctor: the phase name is " + sp.getName());
		System.out.println("Test SpillDoctor: " + ENTER_RETURN +
				sp.getSpillLength() + " is the max spill length " + ENTER_RETURN 
				+ sp.getSpillRecord() + " is the total spill record " + ENTER_RETURN + 
				sp.getSpillTime() + " is the spilled time");
	}
	
	@Test
	public void testMergeDoctor(){
		
//		alp.readAndProcessLog(MAP_LONG);
		MapMergePhase mmp =  (MapMergePhase) alp.getPhaseMap().get("MapMergePhase");

		System.out.println();
		System.out.println("Test MergeDoctor: the phase name is " + mmp.getName());
		System.out.println("Test SignalDoctor: " + ENTER_RETURN +
				mmp.getMergeTime() + " is the the merge time it takes");
	}
	
	@Test 
	public void testInfoDoctor(){
//		alp.readAndProcessLog(MAP_LONG);
		InfoPhase ip = (InfoPhase) alp.getPhasesResult().getInfoPhase();
		
		System.out.println();
		System.out.println("Test InfoDoctor: the phase name is " + ip.getName());
		System.out.println("Test InfoDoctor: " + ENTER_RETURN +
				ip.getCompressLib()+ " is the compressionLib" + ENTER_RETURN);
		
	}
	
//	@Test
//	public void test
	
//	@Test 
	public void testNumberExtraction(){
		
	}

} // public class TestLogParser{


