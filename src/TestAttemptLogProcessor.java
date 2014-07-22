
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.type.TypeFactory;

import edu.duke.starfish.dataflow.hive.tasklog.MRTaskAttemptLog;
import edu.duke.starfish.profile.profileinfo.execution.jobs.MRJobInfo;
import edu.duke.starfish.profile.utils.Constants;
import edu.duke.starfish.workload.ProfileDBManager;
import edu.duke.starfish.workload.storage.model.JobsWithBLOBs;

import java.io.*;
import java.util.*;
import java.util.zip.GZIPInputStream;

import javax.sound.midi.MidiDevice.Info;

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
	public static final String MAP_INCOMPLETE = FOLDER + "MapIncomplete.txt";
	public static final String REDUCE_INCOMPLETE = FOLDER + "ReduceIncomplete.txt";
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

	static String jdbcUrl = "jdbc:mysql://172.22.24.97:3306/starfish_production?user=rfi_test&password=90f55b33e"; // RF
//	static ProfileDBManager dao;


	private MRAttemptLogProcessor alp= new MRAttemptLogProcessor();

	@Before
	public void setUp() throws Exception {
		//ToDo: get connection info from properties instead of hard-wiring
		//ProfileDBManager dao = new ProfileDBManager("org.postgresql.Driver", url);
		//dao = new ProfileDBManager("com.mysql.jdbc.Driver", jdbcUrl);
		
		
//		dao = ProfileDBManager.instance();

//		alp.readAndProcessLog(RECORD_SPILL);
		//		alp.readAndProcessLog(TEST_SPILL);
	}

	@After
	public void tearDown() throws Exception {

	}



	private String getStringFromCompressedBlob(byte[] byteArr) {
		String ret = null;
		try {
			//Blob blob = resultSet.getBlob(column_name);
			//byte[] byteArr = blob.getBytes(1, (int) blob.length());

			// uncompress it
			InputStream inStream = new GZIPInputStream(new ByteArrayInputStream(byteArr));
			ByteArrayOutputStream baoStream2 = new ByteArrayOutputStream();
			byte[] buffer = new byte[512];
			int len;
			while ((len = inStream.read(buffer)) > 0) {
				baoStream2.write(buffer, 0, len);
			}
			ret = baoStream2.toString("UTF-8");
			inStream.close();
			baoStream2.close();
		} catch (Exception e) {
			System.out.println("Exception in ProfileDBManager.getStringFromCompressedBlob: " + e);
			ret = null;
		}

		return ret;
	}
/*

//	@Test
	public void testStudyGenerator() {
		long startTime = 1404950400;
//		long endTime = 1404950450;
		long endTime = 1404950520;
		List<String> JOBLIST = null;
		List<Integer> ioSortMB = new ArrayList<Integer>();
		List<String> taskLogList = new ArrayList<String>();
		List<String> idList = new ArrayList<String>();

		JOBLIST = dao.getMRJobInfosByTimeRange(startTime,endTime);

		//String[] arr = {"job_201405200258_387904"};
		boolean didUpdateSucceed = true;
		for (String jobId : JOBLIST) {
			MRJobInfo mrJobInfo = dao.getMRJobInfo(jobId);
			if (mrJobInfo == null) {
				continue;
			}

			// didUpdateSucceed = dao.putJobFields(mrJobInfo);
			// System.out.println(jobId);
			JobsWithBLOBs jobsWithBLOBs = dao.getJobsWithBLOBsByJid(jobId);
			String logs = getStringFromCompressedBlob(jobsWithBLOBs.getExtraBlobFieldA());
			ObjectMapper objectMapper = new ObjectMapper();
			try {
				List<MRTaskAttemptLog> list = objectMapper.readValue(logs,
						TypeFactory.defaultInstance().constructCollectionType(List.class,
								MRTaskAttemptLog.class));

				for(MRTaskAttemptLog attemptLog : list) {
					if(attemptLog.getSpeed().equalsIgnoreCase("slowest") && attemptLog.getStatus().equalsIgnoreCase("success") && attemptLog.getType().equalsIgnoreCase("map")){
						taskLogList.add(attemptLog.getTaskLog());
						ioSortMB.add(mrJobInfo.getConf().getInt(Constants.MR_SORT_MB, Constants.DEF_SORT_MB));
						idList.add(mrJobInfo.getExecId());
					}
				}

			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
		System.out.println("Printing one log as an example " + ParseUtils.ENTER_RETURN + 
				taskLogList.get(0).toString());
		

//		System.out.println(Arrays.toString(JOBLIST.toArray()));
		System.out.println(Arrays.toString(ioSortMB.toArray()));
		System.out.println(Arrays.toString(idList.toArray()));
		StudyGenerator.EXCEL(taskLogList, idList, ioSortMB);
	}*/

	//	@Test 
	public void testSpillDoctor(){

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
	}

	//	@Test
	public void testMergeDoctor(){

		//		alp.readAndProcessLog(MAP_LONG);
		MapMergePhase mmp =  (MapMergePhase) alp.getPhaseMap().get("MapMergePhase");

		System.out.println();
		System.out.println("Test MergeDoctor: the phase name is " + mmp.getName());
		System.out.println("Test SignalDoctor: " + ENTER_RETURN +
				mmp.getDuration() + " is the the merge time it takes" + ENTER_RETURN +
				mmp.getNumRedTasks() + " is the number of reduce tasks");
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


//		@Test 
	public void testException(){
				alp.readAndProcessLog(ERROR);
		//		lp.readAndProcessLog(VERY_LONG);
//		alp.readAndProcessLog(LINE_RETURN);
		System.out.println();
		System.out.println("Printing Test Exception: ");
		for (String message: alp.getExceptionMap().values()){
			System.out.println("The message at the exception is: " + message);
		}
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
	public void testIncompleteLog(){
		alp.readAndProcessLog(MAP_INCOMPLETE);

		InfoPhase ip = (InfoPhase) alp.getPhasesResult().getInfoPhase();

		System.out.println();
		System.out.println("Test IncompleteLog: the phase name is " + ip.getName());
		System.out.println("The compressionLib is :" +ip.getCompressLib()+  ENTER_RETURN +
				"The other parameters are listed below:  " + ip.getCodecMap() + ENTER_RETURN +
				ip.getInputLog() + ENTER_RETURN + 
				ip.getErrorMap() + ENTER_RETURN + 
				ip.getExceptionMap() + ENTER_RETURN +
				ip.getMemoryList() + ENTER_RETURN +
				ip.getObsoleteOutputMap() + ENTER_RETURN + 
				ip.getTimeSpanList());

	}

	@Test 
	public void testInfoDoctor(){
				alp.readAndProcessLog(ERROR);
		InfoPhase ip = (InfoPhase) alp.getPhasesResult().getInfoPhase();

		System.out.println();
		System.out.println("Test InfoDoctor: the phase name is " + ip.getName());
		System.out.println("Test InfoDoctor: " + ENTER_RETURN +
				ip.getCompressLib()+ " is the compressionLib" + ENTER_RETURN
				+ "attemptId is " + ip.getAttemptID()+ ENTER_RETURN );
		
		System.out.println("The exception message is " + Arrays.toString(ip.getExceptionMap().values().toArray()));
		System.out.println("The exception map is " + Arrays.toString(ip.getExceptionMap().keySet().toArray()));
		System.out.println("The error map is " + Arrays.toString(ip.getErrorMap().values().toArray()));
		System.out.println("The error map keySet is " + Arrays.toString(ip.getErrorMap().keySet().toArray()));
		System.out.println("Test InfoDoctor: the exception message is " + ip.getExceptionLog());

	}

	//	@Test
	//	public void test

	//	@Test 
	public void testNumberExtraction(){

	}

} // public class TestLogParser{


