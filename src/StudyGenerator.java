import java.util.ArrayList;
import java.util.List;



/**
 * A Study Generator that has a static method interface
 * @author Steve Siyang Wang
 *
 */
public class StudyGenerator {

	public static void EXCEL(List<String> attemptLogList, List<String> idList, List<Integer> ioSortMB){
		ExcelWriter ew = new ExcelWriter();
		MRAttemptLogProcessor alp = new MRAttemptLogProcessor();
		List<PhasesResult> prList = new ArrayList<PhasesResult>();

		try{
			if (attemptLogList.size() == ioSortMB.size()){
				for (int i = 0; i<ioSortMB.size(); i++){
					prList.add(alp.readAndProcessLog(attemptLogList.get(i)));
				}
				ew.createSMTimeTable(prList, idList, ioSortMB);
				System.err.println("Size of attemptLogList is " + attemptLogList.size());
				System.err.println("Size of phasesResultList is " + prList.size());
				System.err.println("Size of ioSortMB list is " + ioSortMB.size());
			}
			else {
				System.err.println("Size of two inputs to EXCEl does not match");
			}
		}
		catch (Throwable T){
			T.printStackTrace();
			System.err.println("StudyGenerator failed generate Excel Sheet. Check ");
		}

	}

}
