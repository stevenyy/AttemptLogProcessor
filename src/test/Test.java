package test;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Test {

	public static void main(String[] args){

		String str=" abcd 1234567890 pqr 54";
		String str2 = "ExecReducer: processing 10 rows: used memory = 875392552";
		Pattern p = Pattern.compile("(\\d+)"); 
		Matcher m = p.matcher(str2);
		while(m.find())
		{
			System.out.println(m.group());
//			System.out.println(m.group());
//			System.out.println("printing the matches here :" + m.find());
//			System.out.println("printing the match counter here " + c);
		}

	}

}
