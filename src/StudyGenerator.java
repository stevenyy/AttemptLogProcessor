import java.util.ArrayList;
import java.util.List;



/**
 * A Study Generator that has a static method interface
 * @author Steve Siyang Wang
 *
 */
public class StudyGenerator {

	public static void EXCEL(List<String> ioList, List<String> logList){
		ExcelWriter ew = new ExcelWriter();
		MRAttemptLogProcessor alp = new MRAttemptLogProcessor();
		List<PhasesResult> prList = new ArrayList<PhasesResult>();

		try{
			if (ioList.size() == logList.size()){
				for (int i = 0; i<logList.size(); i++){
					prList.add(alp.readAndProcessLog(logList.get(i)));
				}

				ew.createSMTimeTable(prList, ioList);
			}
			System.err.println("Size of two inputs to EXCEl does not match");
		}
		catch (Throwable T){
			T.printStackTrace();
			System.err.println("StudyGenerator failed generate Excel Sheet. Check ");
		}

	}

}
