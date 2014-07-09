import org.junit.After;
import org.junit.Before;
import org.junit.Test;


public class TestParseUtils {
	
	@Before
	public void setUp() throws Exception {
		
	}
	
	@After
	public void tearDown() throws Exception {

	}
	
	@Test
	public void testExtractNumbers(){
		String message = "kvstart = 0; kvend = 671088; length = 838860";
		System.out.println();
		for (Object obj : ParseUtils.extractNumber(message)){
			System.out.println("Printing from extractNumbers: " + obj.toString());
		}
	}
}
