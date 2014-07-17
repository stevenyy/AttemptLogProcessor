import static org.junit.Assert.assertEquals;

import javax.sound.midi.MidiDevice.Info;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;


public class TestParseUtils {
	
	public static final String FOLDER = "/Users/Hadoop/Desktop/TestData/";
	public static final String MAP_SPILL = FOLDER + "MapSpill.txt";
	public static final String MAP_LONG = FOLDER + "MapLong.txt";
	public static final String ENTER_RETURN = System.getProperty("line.separator");
	
	MRAttemptLogProcessor alp = new MRAttemptLogProcessor();
	
	@Before
	public void setUp() throws Exception {
		
	}
	
	@After
	public void tearDown() throws Exception {

	}
	
	/**
	 * Technically speaking this is a test on InfoDoctor
	 */
	@Test 
	public void testCheckJobInfo(){
		String line = "2014-07-12 22:58:18,362 INFO org.apache.hadoop.mapred.TaskRunner: "
				+ "Creating symlink: /srv/data/disk11/hadoop/mapred/local/taskTracker/mobile/distcache/3821883628819521061_-461103763_765966555/inw-hercules-ha/srv/grid-tmp/hive-mobile/hive_2014-07-12_22-57-45_957_7469990397307196607-1/-mr-10008/dd1569db-69c2-4620-83f3-aa1f0a5a5b5e/map.xml <- /srv/data/disk11/hadoop/mapred/local/taskTracker/mobile/jobcache/job_201405200258_412345/attempt_201405200258_412345_m_000185_0/work/map.xml";
		InfoDoctor id = new InfoDoctor("InfoDoctor");
		id.checkJobInfo(line);
	}
	
	@Test
	public void testGetDate(){
		String line = "2014-07-08 00:43:49,637 WARN mapreduce.Counters: Group org.apache.hadoop.mapred.Task$Counter is deprecated. Use org.apache.hadoop.mapreduce.TaskCounter instead";
		System.out.println("printing the time in long " + ParseUtils.getTime(line));
	}
	
	@Test
	public void testExtractNumbers(){
		String message = "kvstart = 0; kvend = 671088; length = 838860";
		String message2 = "data buffer = 204010960/255013696  3343.35";
		System.out.println();
		for (Object obj : ParseUtils.extractNumber(message2)){
			System.out.println("Printing from extractNumbers: " + obj.toString());
		}
	}
	
/*	!! Incomplete
 * @Test
	public void testExtractNumArray(){
		String message = "kvstart = 0; kvend = 671088; length = 838860";
		System.out.println();
		Integer[] numArray = ParseUtils.extractNumberArray(message);
		System.out.println("Printing from extractNumArray " + numArray);
	}*/
	
	@Test
	public void testSkipLine(){
		String message = "2014-07-08 00:43:50,442 INFO org.apache.hadoop.mapred.TaskRunner:"
				+ " Creating symlink: /srv/data/disk6/hadoop/mapred/local/taskTracker/mtsugawa/distcache/-7631493922812564744_-1485743239_335352318/inw-hercules-ha/tmp/hive-beeswax-mtsugawa/hive_2014-07-07_20-20-53_623_862871815162639044-5957/-mr-10005/df02614c-4de0-49d8-a031-72f22ff3cef5/map.xml <- /srv/data/disk1/hadoop/mapred/local/taskTracker/mtsugawa/jobcache/job_201405200258_371918/attempt_201405200258_371918_m_000057_0/work/map.xml";
		String test2 = "2014-07-08 00:43:51,798 INFO org.apache.hadoop.hive.ql.exec.Utilities: <PERFLOG method=deserializePlan>";
		String test3 = "              <Parent>Id = 1 null<\\Parent>";
		String test4 = "<\\MAP>";
		System.out.println("Printing the test3 string here" + test3);
		boolean result = ParseUtils.skipLine(message, 0);
		boolean result2 = ParseUtils.skipLine(test2, 0);
		boolean result3 = ParseUtils.skipLine(test3, 0);
		boolean result4 = ParseUtils.skipLine(test4, 0);
		assertEquals(false, result);
		assertEquals(false, result2);
		assertEquals(true, result3);
		assertEquals(true, result4);
		
//		MRAttemptLogProcessor alp = new MRAttemptLogProcessor();
		alp.readAndProcessLog(MAP_LONG);
		
		String line = "2014-07-13 08:05:56,146 INFO org.apache.hadoop.mapred.MapTask: Spilling map output: record full = true";
		String target = "full = true";
		
//		System.out.println(line.split(target)[0].split(ParseUtils.SPACE)[7]);
		
	}
	
	@Test 
	public void testGetWordBefore(){
		String line = "2014-07-13 08:05:56,146 INFO org.apache.hadoop.mapred.MapTask: Spilling map output: record full = true";
		String target = "full = true";
		
		String res = ParseUtils.getWordBefore(line, target);
		assertEquals("record", res);
				
	}
	

}
