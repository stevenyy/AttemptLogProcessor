

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;


public class SimpleTest {
	public static void main(String[] arg){
		String dateAndTime1 = "2014-06-20 00:46:00,477";
		String dateAndTime2 = "2014-06-20 00:47:50,478";
		
	    SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss,SSS");
		Date d1 = null;
	    Date d2 = null;
	    try {
	        d1 = format.parse(dateAndTime1);
	        d2 = format.parse(dateAndTime2);
	    } catch (ParseException e) {
	        e.printStackTrace();
	    }
	    long diff = d2.getTime() - d1.getTime();
	    System.out.println("The difference in milisecond is " + diff);
	    
	}
	
	
}
