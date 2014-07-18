/*package edu.duke.starfish.dataflow;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.type.TypeFactory;
import edu.duke.starfish.dataflow.hive.tasklog.MRTaskAttemptLog;
import edu.duke.starfish.profile.profileinfo.execution.MRExecutionStatus;
import edu.duke.starfish.profile.profileinfo.execution.mrtaskattempts.MRMapAttemptInfo;
import edu.duke.starfish.profile.utils.Constants;
import edu.duke.starfish.workload.HiveTableIOInfo;
import edu.duke.starfish.workload.storage.model.JobsWithBLOBs;
import org.json.JSONArray;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.*;
import java.sql.Blob;
import java.sql.ResultSet;
import java.util.*;
import java.util.Map.Entry;
import java.util.zip.GZIPInputStream;

import edu.duke.starfish.workload.ProfileDBManager;
import edu.duke.starfish.workload.WorkflowInfo;
import edu.duke.starfish.profile.profileinfo.execution.jobs.MRJobInfo;
import edu.duke.starfish.profile.profileinfo.execution.jobs.MRJobInfoAnnotation;
import edu.duke.starfish.dataflow.hive.hook.pojo.HiveQueryInfo;
import edu.duke.starfish.dataflow.hive.hook.pojo.HiveStageInfo;
import edu.duke.starfish.dataflow.hive.hook.pojo.HiveTableInfo;
import edu.duke.starfish.dataflow.hive.HiveQueryAllInfo;
import edu.duke.starfish.dataflow.hive.HiveQueryAnnotation;
import edu.duke.starfish.dataflow.hive.HiveQueryInfoLoader;
import edu.duke.starfish.profile.profileinfo.execution.profile.enums.MRCounter;

import edu.duke.starfish.jobopt.params.HadoopParameter;
import edu.duke.starfish.jobopt.rbo.JobParametersHealthCheck;
import edu.duke.starfish.jobopt.rbo.ParameterCheckResponse;

import edu.duke.starfish.profile.profileinfo.execution.jobs.json.profiles.ProfilesModel;

*//**
 * Test the ProfileDBManager
 *
 * @author shivnath
 *//*
public class TestProfileDBManager {

    // static String jdbcUrl = "jdbc:mysql://localhost:3306/starfishapp_dev?user=root&password=resumodnar101";
    // NOTE: this the JDBC URL for the MySQL database at RF
    static String jdbcUrl = "jdbc:mysql://172.22.24.97:3306/starfish_production?user=rfi_test&password=90f55b33e"; // RF
        // From http://jdbc.postgresql.org/documentation/80/connect.html
        //static String jdbcUrl = "jdbc:postgresql://localhost:5432/starfishapp_dev?user=shivnath";


     static ProfileDBManager dao;

    @Before
    public void setUp() throws Exception {
        //ToDo: get connection info from properties instead of hard-wiring

        //ProfileDBManager dao = new ProfileDBManager("org.postgresql.Driver", url);
        //dao = new ProfileDBManager("com.mysql.jdbc.Driver", jdbcUrl);
        dao = ProfileDBManager.instance();

    }

    @After
    public void tearDown() throws Exception {
        if (dao != null) {
            dao.cleanup();
            dao = null;
        }

    }

    //@Test 
    public void testGetMRJobInfo() {

        // MRJobInfo mrJobInfo = dao.getMRJobInfo("job_201304141923_0003");
        MRJobInfo mrJobInfo = dao.getMRJobInfo("job_201312041126_9289");
        if (mrJobInfo != null) {
            System.out.println("MRJobInfo Name: " + mrJobInfo.getName());
            System.out.println("MRJobInfo User: " + mrJobInfo.getUser());

				
        for (MRMapInfo info : mrJobInfo.getMapTasks()) {
		    for (MRMapAttemptInfo attemptInfo : info.getAttempts()) {
			if (attemptInfo.getTruncatedTaskId().equals("m_001601_0")) {
			    System.out.println("TaskTracker for m_001601_0" + 
					       attemptInfo.getTaskTracker());
			}
		    }
		}
		

            String[] ser = mrJobInfo.serializeAsStrings();

            // Using try() will close stream automatically. This version is
            // short, fast (buffered) and enables choosing encoding.

            Writer writerT = null;
            Writer writerP = null;
            try {
                writerT = new BufferedWriter(new OutputStreamWriter(new FileOutputStream("timeline_job_201312041126_9289.json"), "utf-8"));
                writerP = new BufferedWriter(new OutputStreamWriter(new FileOutputStream("profile_job_201312041126_9289.json"), "utf-8"));
                writerT.write(ser[2]);
                writerT.close();
                writerP.write(ser[3]);
                writerP.close();
            } catch (IOException ex) {
                // handle me
                System.out.println("Exception writing to timeline/profile JSON: " + ex);
            }
        } else {
            System.out.println("BUG: MRJobInfo is null");
        }

        // done by tearDown() // dao.cleanup();
    }

    //@Test
    public void testGetHiveQueryInfo() {
        // done by setUp() //ProfileDBManager dao = new ProfileDBManager("org.postgresql.Driver", url);
	
	//String QUERYID = "hadoop_20140611174848_0bb3a35e-b1d5-4161-b38f-a10dda0383ac";
	String QUERYID = "hadoop_20130415033838_84b3a7a1-6758-402b-977f-be11b6a76936";
    
    HiveQueryAllInfo hiveQueryAllInfo = dao.getHiveQueryAllInfo(QUERYID);
        if (hiveQueryAllInfo == null) {
            System.out.println("BUG: HiveQueryAllInfo is null");
        } else {
            HiveQueryInfo hiveQueryInfo = hiveQueryAllInfo.getHiveQueryInfo();
            if (hiveQueryInfo == null) {
                System.out.println("BUG: HiveQueryInfo is null");
            } else {
		
                System.out.println("HiveQueryInfo QueryString: " + hiveQueryInfo.getQueryString());
                System.out.println("HiveQueryInfo StartTime: " + hiveQueryInfo.getStartTime());
                System.out.println("HiveQueryInfo EndTime: " + hiveQueryInfo.getEndTime());
                //System.out.println("HiveQueryAnnotation: " + 
		//dao.recomputeAndReplaceHiveQueryAnnotation(QUERYID));

                List<MRJobInfo> mrJobInfos = hiveQueryAllInfo.getAllMRJobInfos();
                if (mrJobInfos == null) {
                    System.out.println("BUG: mrJobInfos is null");
                } else {
                    System.out.println("There are " + mrJobInfos.size() + " MR stages in this query.");
                    for (MRJobInfo jobInfo : mrJobInfos) {
			if (jobInfo == null) {
			    System.out.println("MRJobInfo is NULL");
			}
			else {
			    System.out.println("MRJobInfo Name: " + jobInfo.getName());
			    System.out.println("MRJobInfo User: " + jobInfo.getUser());
			}
                    }
                }
            }
        }

        // done by tearDown() // dao.cleanup();
    }

    // HashMap keyed by Input Path obtained from the input paths of Hive MapReduce Stages
// 
//   Entries must have <Key = TablePath, 
//                              Value = <
//                                         TableName, // populated during postprocessing
//                                         TOTAL_HDFS_READ_IO, 
//                                         NUM_READER_MR_JOBS
//                                      >
//                     >
    class HdfsReadIO_Path {
        public String tableName; // populated during postprocessing
        public long totalHdfsReadIO;
        public long numReaderMRJobs;

        // public long numReaderMRTasks;
        public HdfsReadIO_Path(long totalHdfsReadIO, long numReaderMRJobs) {
            this.tableName = null;
            this.totalHdfsReadIO = totalHdfsReadIO;
            this.numReaderMRJobs = numReaderMRJobs;
            // numReaderMRTasks = 0l;
        }
    }

    // HashMap keyed by Full Table Names obtained from the full table name fields in Hive Queries
// 
//   Entries must have <Key = TableName,
//                              Value = <
//                                         tablePath  //  can be NULL
//                                         NUM_READER_HIVE_QUERIES                              
//                                         TOTAL_HDFS_READ_IO // populated during postprocessing
//                                       >
//                      >
    class HdfsReadIO_Table {
        public String tablePath;   //  can be NULL (e.g., for views)
        public long totalHdfsReadIO;
        public long numReaderHiveQueries;

        // public long numReaderMRTasks;
        public HdfsReadIO_Table(String tablePath, long numReaderHiveQueries) {
            this.tablePath = tablePath;  //  can be NULL (e.g., for views)
            this.totalHdfsReadIO = 0l;
            this.numReaderHiveQueries = numReaderHiveQueries;
            // numReaderMRTasks = 0l;
        }
    }

    //    @Test 
    public void testGetHiveQueries() {
        // From http://jdbc.postgresql.org/documentation/80/connect.html
        //	String url = "jdbc:postgresql://localhost:5432/starfishapp_dev?user=shivnath";
        // String url = "jdbc:mysql://172.22.24.97:3306/starfishapp_prod?user=rfi_test&password=90f55b33e";
        // String url = "jdbc:mysql://172.22.24.97:3306/test?user=rfi_test&password=90f55b33e";
        String url = "jdbc:mysql://localhost:3306/starfishapp_dev?user=root&password=resumodnar101";
        // String url = "jdbc:mysql://localhost:3306/starfishapp_prod?user=root&password=resumodnar101";

        //	ProfileDBManager dao = new ProfileDBManager("org.postgresql.Driver", url);
        // done by setUp() //ProfileDBManager dao = new ProfileDBManager("com.mysql.jdbc.Driver", url);

        // dao.runQueryOnHiveQueryTable();

        // HashMap keyed by Full Table Names obtained from the full table name fields in Hive Queries
        Map<String, HdfsReadIO_Table> tableMap = new HashMap<String, HdfsReadIO_Table>();
        // HashMap keyed by Input Path obtained from the input paths of Hive MapReduce Stages
        Map<String, HdfsReadIO_Path> pathMap = new HashMap<String, HdfsReadIO_Path>();
	
	
mysql> select UNIX_TIMESTAMP('2013-05-18 00:00:00');
+---------------------------------------+
| UNIX_TIMESTAMP('2013-05-18 00:00:00') |
+---------------------------------------+
|                            1368860400 |
+---------------------------------------+

mysql> select UNIX_TIMESTAMP('2013-05-21 00:00:00');
+---------------------------------------+
| UNIX_TIMESTAMP('2013-05-21 00:00:00') |
+---------------------------------------+
|                            1369119600 |
+---------------------------------------+
	

        long startTime = 1368860400;
        long endTime = 1369119600;

        List<HiveQueryAllInfo> query_list =
          dao.getHiveQueryAllInfos(startTime, endTime);

        if (query_list != null) {
            for (HiveQueryAllInfo hqa : query_list) {
                System.out.println("Query ID: " + hqa.getHiveQueryInfo().getQueryId());

                for (HiveTableInfo tableInfo : hqa.getHiveQueryInfo().getFullTableNameToInfos().values()) {
		     
		       if (tableInfo.getPath() == null) 
		       System.out.println("   TableName: " + tableInfo.getFullTableName() + ", TablePath: NULL");
		       else System.out.println("   TableName: " + tableInfo.getFullTableName() + ", TablePath: " +  tableInfo.getPath());
		    

                    HdfsReadIO_Table tableEntry = (HdfsReadIO_Table) tableMap.get(tableInfo.getFullTableName());
                    if (tableEntry == null) {
                        tableMap.put(tableInfo.getFullTableName(), new HdfsReadIO_Table(tableInfo.getPath(), 1l));
                    } else {
                        // numReaderHiveQueries += 1;
                        tableEntry.numReaderHiveQueries += 1;
                    }
                } // for tableInfo in query

                for (HiveStageInfo stage : hqa.getHiveQueryInfo().getHiveStageInfos()) {
                    String jobID = stage.getHadoopJobId();
                    if (jobID != null && !jobID.trim().isEmpty()) {

                        System.out.println("    Stage JobID: " + jobID);
                        MRJobInfo jobInfo = hqa.getMRJobInfo(jobID);
                        if (jobInfo == null) {
                            System.out.println("MRJobInfo is NULL");
                            continue;
                        }

                        for (String inputPath : stage.getInputPaths()) {
                            if (inputPath == null) {
                                System.out.println("InputPath is NULL");
                                continue;
                            }
                            long hdfsBytesRead =
                              jobInfo.getMapCounterByInput(inputPath,
                                                            MRCounter.HDFS_BYTES_READ)
                                + jobInfo.getMapCounterByInput(inputPath,
                                                                MRCounter.S3_BYTES_READ);

                            System.out.println("        InputPath: " + inputPath + ", HDFSBytesRead = " + hdfsBytesRead);

                            HdfsReadIO_Path pathEntry = (HdfsReadIO_Path) pathMap.get(inputPath);
                            if (pathEntry == null) {
                                pathMap.put(inputPath, new HdfsReadIO_Path(hdfsBytesRead, 1l));
                            } else {
                                pathEntry.totalHdfsReadIO += hdfsBytesRead;
                                pathEntry.numReaderMRJobs += 1;
                            }

                        } // for inputPath

                    } // jobID is valid
                } // for stage
            } // for hqa
        } // if valid query_list
        else {
            System.out.println("No queries started in this time range");
        }

        System.out.println("=================================");

        // Print the size of HashMaps
        System.out.println("TableMap size = " + tableMap.size());
        System.out.println("PathMap size = " + pathMap.size());

        Set<Entry<String, HdfsReadIO_Path>> pathSet = pathMap.entrySet();
        Set<Entry<String, HdfsReadIO_Table>> tableSet = tableMap.entrySet();
        for (Entry pathKV : pathSet) {

            String inputPath = (String) pathKV.getKey();
            HdfsReadIO_Path pathEntry = (HdfsReadIO_Path) pathKV.getValue();

            String matchingTableName = null;
            HdfsReadIO_Table matchingTableEntry = null;

            for (Entry tableKV : tableSet) {

                String tableName = (String) tableKV.getKey();
                HdfsReadIO_Table tableEntry = (HdfsReadIO_Table) tableKV.getValue();

                // check if the path corresponds to this table
                if (tableEntry.tablePath != null && inputPath.startsWith(tableEntry.tablePath)) {
                    if (matchingTableEntry != null) {
                        // store the longest match
                        if (tableEntry.tablePath.length() > matchingTableEntry.tablePath.length()) {
                            matchingTableName = tableName;
                            matchingTableEntry = tableEntry;
                        }
                    } else {
                        matchingTableName = tableName;
                        matchingTableEntry = tableEntry;
                    }
                }
            } // for tableKV

            // update the table and path entries
            if (matchingTableEntry != null) {
                // table for path entry
                pathEntry.tableName = matchingTableName;
                matchingTableEntry.totalHdfsReadIO += pathEntry.totalHdfsReadIO;
            }

        } // for pathKV

        // Print the contents of the maps
        try {

            // true tells to append data
            // FileWriter fstream = new FileWriter("hive_tables.csv", true);
            FileWriter fstream = new FileWriter("hive_tables.csv", false);
            BufferedWriter out = new BufferedWriter(fstream);

            for (Entry tableKV : tableSet) {
                String tableName = (String) tableKV.getKey();
                // System.out.println();
                // System.out.println("TableName: " + tableName);
                HdfsReadIO_Table tableEntry = (HdfsReadIO_Table) tableKV.getValue();
                // System.out.println("   HdfsReadIO: " + tableEntry.totalHdfsReadIO);
                // System.out.println("   NumReaderHiveQueries: " + tableEntry.numReaderHiveQueries);
                // System.out.println();
                String inputPath = "None";
                if (tableEntry.tablePath != null)
                    inputPath = tableEntry.tablePath;

                out.write(tableName + "," + inputPath + "," + tableEntry.totalHdfsReadIO
                            + "," + tableEntry.numReaderHiveQueries + "," +
                            startTime + "," + endTime + "\n");
            }
            out.close();
            // true tells to append data
            // FileWriter fstream = new FileWriter("hive_tables.csv", true);
            fstream = new FileWriter("hive_input_paths.csv", false);
            out = new BufferedWriter(fstream);

            for (Entry pathKV : pathSet) {
                String inputPath = (String) pathKV.getKey();
                HdfsReadIO_Path pathEntry = (HdfsReadIO_Path) pathKV.getValue();
                String tableName = "None";
                if (pathEntry.tableName != null)
                    tableName = pathEntry.tableName;
		
		 
		   System.out.println();
		   System.out.println("InputPath: " + inputPath); 
		   System.out.println("TableName: " + pathEntry.tableName); 
		   System.out.println("   HdfsReadIO: " + pathEntry.totalHdfsReadIO);
		   System.out.println("   NumReaderMRJobs: " + pathEntry.numReaderMRJobs);
		   System.out.println();
		
                out.write(inputPath + "," + tableName + "," + pathEntry.totalHdfsReadIO
                            + "," + pathEntry.numReaderMRJobs + "," +
                            startTime + "," + endTime + "\n");
            }
            out.close();
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
        }

        // done by tearDown() // dao.cleanup();
    } // testGetHiveQueries

    //@Test
    public void testGetMRJobInfosByName() {


        // done by setUp() //ProfileDBManager dao = new ProfileDBManager("com.mysql.jdbc.Driver", jdbcUrl);

        //
        // NOTE: getMRJobInfosByJobName is an adapter function in ProfileDBManager to fetch all
        //  the MapReduce JobIDs whose job names have the following job name as a substring
        //
        //String JobNameSubStr = "Generate modeling_rtb_mv (full)";
        String JobNameSubStr = "create table williams_sonoma_pvt...data_date(Stage-3)";
        // get the list of matching MR job ids
        List<String> jobid_list = dao.getMRJobInfosByJobName(JobNameSubStr);

        if (jobid_list == null) {
            System.out.println("ERROR: No matching jobs found. Exiting!");
            return;
        }

        int seqNum = 1;
        for (String jobID : jobid_list) {
            //System.out.println("MR JobID: " + jobID);

            // Get the MRJobInfo object
            MRJobInfo jobInfo = dao.getMRJobInfo(jobID);
            if (jobInfo == null) {
                System.out.println("ERROR: jobID is NULL");
                continue;
            }

            //System.out.println("Duration: " + jobInfo.getDuration() + ", Status = " + jobInfo.getStatus());
            printJobEfficiency(jobInfo, seqNum);
            seqNum++;

            printJobTimelineHistograms(jobInfo, 5  interval in minutes );
        } // for jobID

    } // testGetMRJobInfosByName() 


    *//**
     * Compute the Job's map and reduce efficiency
     *//*
    public static void printJobEfficiency(edu.duke.starfish.profile.profileinfo.execution.jobs.MRJobInfo jobInfo, int seqNum) {

        long succTime, killTime, failTime;
        double total;
        succTime = jobInfo.getTotalMapSlotDuration(edu.duke.starfish.profile.profileinfo.execution.MRExecutionStatus.SUCCESS);
        killTime = jobInfo.getTotalMapSlotDuration(edu.duke.starfish.profile.profileinfo.execution.MRExecutionStatus.KILLED);
        failTime = jobInfo.getTotalMapSlotDuration(edu.duke.starfish.profile.profileinfo.execution.MRExecutionStatus.FAILED);
        total = succTime + killTime + failTime;
        double mapEfficiency = 1.0;
        if (total > 0)
            mapEfficiency = succTime / total;
        succTime = jobInfo.getTotalReduceSlotDuration(edu.duke.starfish.profile.profileinfo.execution.MRExecutionStatus.SUCCESS);
        killTime = jobInfo.getTotalReduceSlotDuration(edu.duke.starfish.profile.profileinfo.execution.MRExecutionStatus.KILLED);
        failTime = jobInfo.getTotalReduceSlotDuration(edu.duke.starfish.profile.profileinfo.execution.MRExecutionStatus.FAILED);
        total = succTime + killTime + failTime;
        double reduceEfficiency = 1.0;
        if (total > 0)
            reduceEfficiency = succTime / total;

        //   { x: 0.25, y: 300, jobinfo: 'job002823 at 20140122 16:30', size: 16384, shape: 'circle'},
        double size = seqNum * seqNum * seqNum;
        double dur = jobInfo.getDuration() / (60 * 1000.0);
        long rounded_dur = (long) dur;
        System.out.println("{x:" + mapEfficiency + ", y:" + reduceEfficiency +
                             ", jobinfo: '" + jobInfo.getExecId() + "(" + rounded_dur + " minutes on " + jobInfo.getStartTime() +
                             ")', size: " + size + ", shape: 'circle'}");
    }

    public static void printJobTimelineHistograms(edu.duke.starfish.profile.profileinfo.execution.jobs.MRJobInfo jobInfo, int intervalInMinutes) {

        System.out.println("=============MAP TIMELINE=============");
        edu.duke.starfish.profile.profileinfo.execution.jobs.TimelineHistogram hist =
	    edu.duke.starfish.profile.profileinfo.execution.jobs.
	    TimelineHistogram.generate(jobInfo.getMapTasks(), jobInfo.getStartTime().getTime(), jobInfo.getEndTime().getTime());
										  
        if (hist != null)
            hist.print();

        System.out.println("=============REDUCE TIMELINE=============");
        hist =   edu.duke.starfish.profile.profileinfo.execution.jobs.
	    TimelineHistogram.generate(jobInfo.getReduceTasks(), jobInfo.getStartTime().getTime(), jobInfo.getEndTime().getTime());
        if (hist != null)
            hist.print();

        System.out.println("==========================");
    }

    //@Test 
	public void testGetWorkflowInstanceInfo() {
	
	WorkflowInstanceInfo winfo = dao.getWorkflowInstanceInfo("20140521T173342Z--3521673227066310335");
	if (winfo == null) {
	    System.out.println("failed");
	}
	else {
	    System.out.println(winfo.toJson());
	}

    } // public void testGetWorkflowInstanceInfo() {
    
    //@Test 
	public void testGetWorkflowCompare() {
	
	String[] wids = new String[3];
	wids[0] = "20140211T173342Z-642142781499249999";
	wids[1] = "20140213T173342Z-642144487281324881";
	wids[2] = "20140212T173342Z-642143634390287440";
	String jsonStr = WorkflowInfo.toJson(wids[0], wids);
	
	if (jsonStr == null) {
	    System.out.println("failed");
	}
	else {
	    System.out.println(jsonStr);
	}

    } // public void testGetWorkflowInstanceInfo() {

    //@Test 
	public void dataTransformationAlexandra1() {

	String USERNAME = "sandra";
	String QUERYID = "hue_20140122031515_e807bd5a-84b9-4eab-aa55-01deb1bfaabf";
	String JOBID = "job_201401090205_74783";
	String QUEUENAME = "Advertising";
	String SUBMITHOST = "bnb-77.62.43.7";
	String QUERYSTRING = "SELECT country, SUM(3) AS uus FROM (SELECT dim_lookup('countries', country_code, 'country_name') AS country, user_id FROM TABLE TABLESAMPLE (BUCKET 5 OUT OF 16 ON request_id) a WHERE data_date >= 20131201 AND data_date <= 20131231 AND user_id > 0 AND dim_lookup('countries', country_code, 'country_name') <> '' GROUP BY  dim_lookup('countries', country_code, 'country_name'), user_id ) q GROUP BY  country";

	boolean didUpdateSucceed = false; 
        MRJobInfo mrJobInfo = dao.getMRJobInfo(JOBID);
        if (mrJobInfo == null)
	    return;
	
	//System.out.println("MRJobInfo Name: " + mrJobInfo.getName());
	//System.out.println("MRJobInfo User: " + mrJobInfo.getUser());
	
	edu.duke.starfish.workload.storage.model.JobsWithBLOBs
	    jobs = dao.getJobsWithBLOBsByJid(mrJobInfo.getExecId());
	
	//System.out.println("Old Annotation: " + jobs.getAnnotation());
	
	MRJobInfoAnnotation annotation = mrJobInfo.getAnnotation();
	annotation.setUserName(USERNAME);	
	annotation.setQueue(QUEUENAME);
	annotation.setSubmitHost(SUBMITHOST);
	
	String problem = "A MapReduce job in this query was killed";
	String[] causes = {"Data skew in group-by of expression 'dim_lookup('countries', country_code, 'country_name')'"};
	String[] actions = {"Set hive.groupby.skewindata to true to enable skew-aware group-by processing","Use the query's Where clause to filter out any values not needed in the group-by"};
	
	annotation.addErr(problem, causes, actions);
	
	//System.out.println("New Annotation: " + mrJobInfo.getAnnotation().toJson());
	didUpdateSucceed = dao.putJobFields(mrJobInfo);
	
	if (!didUpdateSucceed) {
	    System.out.println("FAILED!");	    
	    return;
	}
	
	HiveQueryAllInfo hiveQueryAllInfo = dao.getHiveQueryAllInfo(QUERYID);
	
	if (hiveQueryAllInfo != null) {
	    
	    // this will also populate the HiveQueryAnnotation
	    HiveQueryInfoLoader.extractInfoFromJobHistory(hiveQueryAllInfo);
	    HiveQueryAnnotation hive_annotation = 
		hiveQueryAllInfo.getAnnotation();
	    hive_annotation.addErr(problem, causes, actions);
	    hive_annotation.setUser(USERNAME);
	    hive_annotation.setQueue(QUEUENAME);
	    
	    hiveQueryAllInfo.getHiveQueryInfo().setQueryString(QUERYSTRING);
	    
	    //System.out.println("New Hive Annotation: " + 
	    //hive_annotation.toJson());
	    didUpdateSucceed = dao.putHiveQueryFields(hiveQueryAllInfo);
	}
	    
	if (!didUpdateSucceed)
	    System.out.println("FAILED!");	    
	else System.out.println("SUCCESS!");
    }

    //@Test 
	public void dataTransformationAlexandraGOOD1() {

	String QUEUENAME = "Advertising";
	String SUBMITHOST = "bnb-77.62.43.7";

	String QUERYSTRING = "SELECT country, SUM(3) AS uus FROM (SELECT dim_lookup('countries', country_code, 'country_name') AS country, user_id FROM TABLE TABLESAMPLE (BUCKET 5 OUT OF 16 ON request_id) a WHERE data_date >= 20131201 AND data_date <= 20131231 AND user_id > 0 AND dim_lookup('countries', country_code, 'country_name') <> '' GROUP BY  dim_lookup('countries', country_code, 'country_name'), user_id ) q GROUP BY  country";

	String USERNAME = "sandra";
	String QUERYID = "hue_20140126235353_67fce2a2-c875-4885-89f3-357729036ecd";
	List<String> JOBLIST = new ArrayList<String>();
	JOBLIST.add("job_201401090205_109960");
	JOBLIST.add("job_201401090205_110215");
	JOBLIST.add("job_201401090205_110520");
	JOBLIST.add("job_201401090205_110545");
	
	boolean didUpdateSucceed = false; 
	for (String jobId : JOBLIST) {
	    MRJobInfo mrJobInfo = dao.getMRJobInfo(jobId);
	    if (mrJobInfo == null)
		return;

	    //System.out.println("MRJobInfo Name: " + mrJobInfo.getName());
	    //System.out.println("MRJobInfo User: " + mrJobInfo.getUser());
	    
	    edu.duke.starfish.workload.storage.model.JobsWithBLOBs
		jobs = dao.getJobsWithBLOBsByJid(mrJobInfo.getExecId());
	
	    //System.out.println("Old Annotation: " + jobs.getAnnotation());
	    
	    MRJobInfoAnnotation annotation = mrJobInfo.getAnnotation();
	    annotation.setUserName(USERNAME);
	    annotation.setQueue(QUEUENAME);
	    annotation.setSubmitHost(SUBMITHOST);
	
	    //System.out.println("New Annotation: " + mrJobInfo.getAnnotation().toJson());
	    didUpdateSucceed = dao.putJobFields(mrJobInfo);
	    
	    if (!didUpdateSucceed) {
		System.out.println("FAILED!");	    
		return;
	    }
	}
	
	didUpdateSucceed = false; 
	HiveQueryAllInfo hiveQueryAllInfo = dao.getHiveQueryAllInfo(QUERYID);
	    
	if (hiveQueryAllInfo != null) {
	    
	    // this will also populate the HiveQueryAnnotation
	    HiveQueryInfoLoader.extractInfoFromJobHistory(hiveQueryAllInfo);
	    HiveQueryAnnotation hive_annotation = 
		hiveQueryAllInfo.getAnnotation();
	    hive_annotation.setUser(USERNAME);
	    hive_annotation.setQueue(QUEUENAME);

	    hiveQueryAllInfo.getHiveQueryInfo().setQueryString(QUERYSTRING);

	    didUpdateSucceed = dao.putHiveQueryFields(hiveQueryAllInfo);
	}
	    
	if (!didUpdateSucceed)
	    System.out.println("FAILED!");	    
	else System.out.println("SUCCESS!");
    }

    //@Test 
	public void dataTransformationAlexandraGOOD2() {

	String QUEUENAME = "Advertising";
	String SUBMITHOST = "bnb-77.62.43.7";

	String QUERYSTRING = "SELECT country, SUM(3) AS uus FROM (SELECT dim_lookup('countries', country_code, 'country_name') AS country, user_id FROM TABLE TABLESAMPLE (BUCKET 5 OUT OF 16 ON request_id) a WHERE data_date >= 20131201 AND data_date <= 20131231 AND user_id > 0 AND dim_lookup('countries', country_code, 'country_name') <> '' GROUP BY  dim_lookup('countries', country_code, 'country_name'), user_id ) q GROUP BY  country";

	String USERNAME = "sandra";
	String QUERYID = "hue_20140129020000_444d2eaa-10cc-45e4-97b4-1391ecaae5c9";
	List<String> JOBLIST = new ArrayList<String>();
	JOBLIST.add("job_201401280343_7868");
	JOBLIST.add("job_201401280343_8224");
	JOBLIST.add("job_201401280343_8437");
	JOBLIST.add("job_201401280343_8452");
	
	boolean didUpdateSucceed = false; 
	for (String jobId : JOBLIST) {
	    MRJobInfo mrJobInfo = dao.getMRJobInfo(jobId);
	    if (mrJobInfo == null)
		return;

	    //System.out.println("MRJobInfo Name: " + mrJobInfo.getName());
	    //System.out.println("MRJobInfo User: " + mrJobInfo.getUser());
	    
	    edu.duke.starfish.workload.storage.model.JobsWithBLOBs
		jobs = dao.getJobsWithBLOBsByJid(mrJobInfo.getExecId());
	
	    //System.out.println("Old Annotation: " + jobs.getAnnotation());
	    
	    MRJobInfoAnnotation annotation = mrJobInfo.getAnnotation();
	    annotation.setUserName(USERNAME);
	    annotation.setQueue(QUEUENAME);
	    annotation.setSubmitHost(SUBMITHOST);
	
	    //System.out.println("New Annotation: " + mrJobInfo.getAnnotation().toJson());
	    didUpdateSucceed = dao.putJobFields(mrJobInfo);
	    
	    if (!didUpdateSucceed) {
		System.out.println("FAILED!");	    
		return;
	    }
	}
	
	didUpdateSucceed = false; 
	HiveQueryAllInfo hiveQueryAllInfo = dao.getHiveQueryAllInfo(QUERYID);
	    
	if (hiveQueryAllInfo != null) {
	    
	    // this will also populate the HiveQueryAnnotation
	    HiveQueryInfoLoader.extractInfoFromJobHistory(hiveQueryAllInfo);
	    HiveQueryAnnotation hive_annotation = 
		hiveQueryAllInfo.getAnnotation();
	    hive_annotation.setUser(USERNAME);
	    hive_annotation.setQueue(QUEUENAME);

	    hiveQueryAllInfo.getHiveQueryInfo().setQueryString(QUERYSTRING);

	    didUpdateSucceed = dao.putHiveQueryFields(hiveQueryAllInfo);
	}
	    
	if (!didUpdateSucceed)
	    System.out.println("FAILED!");	    
	else System.out.println("SUCCESS!");
    }

    //@Test 
	public void dataTransformationGOODMMalpani() {

	String QUEUENAME = "Advertising";
	String SUBMITHOST = "bnb-77.62.43.7";

	String USERNAME = "mmichael";
	String QUERYID = "hue_20140210012121_7795de47-c021-4c6a-9c73-710f7a50ec99";
	List<String> JOBLIST = new ArrayList<String>();
	//["job_201401280343_88667","job_201401280343_89326"]
	JOBLIST.add("job_201401280343_88667");
	JOBLIST.add("job_201401280343_89326");
	String QUERYSTRING = "SELECT get_json_object(r.attributes, '$.id') AS app, sum(1) AS wins FROM TABLE r JOIN TABLE w ON r.ads[4] = w.ads[4] AND r.data_date=\"20140128\" AND w.data_date=\"20140128\" WHERE r.data_date=\"20140128\" AND w.data_date=\"20140128\" AND r.ads[4] = w.ads[4] AND w.account_id=3623 AND r.account_id=3623 AND r.ads[4] <> 0 AND r.ads[4] is NOT null GROUP BY  get_json_object(r.attributes, '$.id')";

	boolean didUpdateSucceed = false; 
	for (String jobId : JOBLIST) {
	    MRJobInfo mrJobInfo = dao.getMRJobInfo(jobId);
	    if (mrJobInfo == null)
		return;

	    //System.out.println("MRJobInfo Name: " + mrJobInfo.getName());
	    //System.out.println("MRJobInfo User: " + mrJobInfo.getUser());
	    
	    edu.duke.starfish.workload.storage.model.JobsWithBLOBs
		jobs = dao.getJobsWithBLOBsByJid(mrJobInfo.getExecId());
	
	    //System.out.println("Old Annotation: " + jobs.getAnnotation());
	    
	    MRJobInfoAnnotation annotation = mrJobInfo.getAnnotation();
	    annotation.setUserName(USERNAME);
	    annotation.setQueue(QUEUENAME);
	    annotation.setSubmitHost(SUBMITHOST);
	
	    //System.out.println("New Annotation: " + mrJobInfo.getAnnotation().toJson());
	    didUpdateSucceed = dao.putJobFields(mrJobInfo);
	    
	    if (!didUpdateSucceed) {
		System.out.println("FAILED!");	    
		return;
	    }
	}
	
	didUpdateSucceed = false; 
	HiveQueryAllInfo hiveQueryAllInfo = dao.getHiveQueryAllInfo(QUERYID);
	    
	if (hiveQueryAllInfo != null) {
	    
	    // this will also populate the HiveQueryAnnotation
	    HiveQueryInfoLoader.extractInfoFromJobHistory(hiveQueryAllInfo);
	    HiveQueryAnnotation hive_annotation = 
		hiveQueryAllInfo.getAnnotation();
	    hive_annotation.setUser(USERNAME);
	    hive_annotation.setQueue(QUEUENAME);

	    hiveQueryAllInfo.getHiveQueryInfo().setQueryString(QUERYSTRING);

	    didUpdateSucceed = dao.putHiveQueryFields(hiveQueryAllInfo);
	}
	    
	if (!didUpdateSucceed)
	    System.out.println("FAILED!");	    
	else System.out.println("SUCCESS!");
  }

    //@Test 
	public void dataTransformationMMalpaniBad1() {

	String USERNAME = "mmichael";
	String QUEUENAME = "Advertising";
	String SUBMITHOST = "bnb-77.62.43.7";
	String QUERYSTRING = "SELECT get_json_object(r.attributes, '$.id') AS app, sum(1) AS wins FROM TABLE r JOIN TABLE w ON r.ads[4] = w.ads[4] AND r.data_date=\"20140128\" AND w.data_date=\"20140128\" WHERE r.data_date=\"20140128\" AND w.data_date=\"20140128\" AND r.ads[4] = w.ads[4] AND w.account_id=3623 AND r.account_id=3623 GROUP BY  get_json_object(r.attributes, '$.id')";
	
	boolean didUpdateSucceed = false; 
        MRJobInfo mrJobInfo = dao.getMRJobInfo("job_201401280343_66728");
        if (mrJobInfo == null)
	    return;

	edu.duke.starfish.workload.storage.model.JobsWithBLOBs
	    jobs = dao.getJobsWithBLOBsByJid(mrJobInfo.getExecId());
	
	//System.out.println("Old Annotation: " + jobs.getAnnotation());
	    
	MRJobInfoAnnotation annotation = mrJobInfo.getAnnotation();
	annotation.setUserName(USERNAME);
	annotation.setQueue(QUEUENAME);
	annotation.setSubmitHost(SUBMITHOST);
	
	String problem = "A MapReduce job in this query was killed";
	String[] causes = {"Data skew in join of expressions \'r.ads[4]\' and \'w.ads[4]\'"};
	String[] actions = {"Set hive.optimize.skewjoin to true to enable skew-aware join processing", "Use the query\'s Where clause to filter out any values not needed in the join", "Use the following parameters to fine-tune Hive\'s skew-aware join processing: hive.skewjoin.key (current value is 100000), hive.skewjoin.mapjoin.min.split (current value is 33554432), hive.skewjoin.mapjoin.map.tasks (current value is 10000)"};
	
	annotation.addErr(problem, causes, actions);
	
	//System.out.println("New Annotation: " + mrJobInfo.getAnnotation().toJson());
	didUpdateSucceed = dao.putJobFields(mrJobInfo);
	
	if (!didUpdateSucceed) {
	    System.out.println("FAILED!");	    
	    return;
	}
	
	HiveQueryAllInfo hiveQueryAllInfo =
	    dao.getHiveQueryAllInfo("hue_20140206145959_55a6fab7-1560-45a4-909d-c213de7c52f9");
	if (hiveQueryAllInfo != null) {
	    
	    // this will also populate the HiveQueryAnnotation
	    HiveQueryInfoLoader.extractInfoFromJobHistory(hiveQueryAllInfo);
	    HiveQueryAnnotation hive_annotation = 
		hiveQueryAllInfo.getAnnotation();
	    hive_annotation.addErr(problem, causes, actions);
	    hive_annotation.setUser(USERNAME);	    
	    hive_annotation.setQueue(QUEUENAME);

	    hiveQueryAllInfo.getHiveQueryInfo().setQueryString(QUERYSTRING);

	    //System.out.println("New Hive Annotation: " + 
	    //hive_annotation.toJson());
	    didUpdateSucceed = dao.putHiveQueryFields(hiveQueryAllInfo);
	}
	    
	if (!didUpdateSucceed)
	    System.out.println("FAILED!");	    
	else System.out.println("SUCCESS!");
    }

    //@Test 
	public void dataTransformationMMalpaniBad2() {

	String USERNAME = "mmichael";
	String QUEUENAME = "Advertising";
	String SUBMITHOST = "bnb-77.62.43.7";
	String QUERYSTRING = "SELECT get_json_object(r.attributes, '$.id') AS app, sum(1) AS wins FROM TABLE r JOIN TABLE w ON r.ads[4] = w.ads[4] AND r.data_date=\"20140128\" AND w.data_date=\"20140128\" WHERE r.data_date=\"20140128\" AND w.data_date=\"20140128\" AND r.ads[4] = w.ads[4] AND w.account_id=3623 AND r.account_id=3623 GROUP BY  get_json_object(r.attributes, '$.id')";
	
	boolean didUpdateSucceed = false; 
        MRJobInfo mrJobInfo = dao.getMRJobInfo("job_201401280343_66727");
        if (mrJobInfo == null)
	    return;

	edu.duke.starfish.workload.storage.model.JobsWithBLOBs
	    jobs = dao.getJobsWithBLOBsByJid(mrJobInfo.getExecId());
	
	//System.out.println("Old Annotation: " + jobs.getAnnotation());
	    
	MRJobInfoAnnotation annotation = mrJobInfo.getAnnotation();
	annotation.setUserName(USERNAME);
	annotation.setQueue(QUEUENAME);
	annotation.setSubmitHost(SUBMITHOST);
	
	String problem = "A MapReduce job in this query was killed";
	String[] causes = {"Data skew in join of expressions \'r.ads[4]\' and \'w.ads[4]\'"};
	String[] actions = {"Set hive.optimize.skewjoin to true to enable skew-aware join processing", "Use the query\'s Where clause to filter out any values not needed in the join", "Use the following parameters to fine-tune Hive\'s skew-aware join processing: hive.skewjoin.key (current value is 100000), hive.skewjoin.mapjoin.min.split (current value is 33554432), hive.skewjoin.mapjoin.map.tasks (current value is 10000)"};
	
	annotation.addErr(problem, causes, actions);
	
	//System.out.println("New Annotation: " + mrJobInfo.getAnnotation().toJson());
	didUpdateSucceed = dao.putJobFields(mrJobInfo);
	
	if (!didUpdateSucceed) {
	    System.out.println("FAILED!");	    
	    return;
	}
	
	HiveQueryAllInfo hiveQueryAllInfo =
	    dao.getHiveQueryAllInfo("hue_20140206145959_0f190861-6200-45cd-b9b7-d33f67d37b71");
	if (hiveQueryAllInfo != null) {
	    
	    // this will also populate the HiveQueryAnnotation
	    HiveQueryInfoLoader.extractInfoFromJobHistory(hiveQueryAllInfo);
	    HiveQueryAnnotation hive_annotation = 
		hiveQueryAllInfo.getAnnotation();
	    hive_annotation.addErr(problem, causes, actions);
	    hive_annotation.setUser(USERNAME);	    
	    hive_annotation.setQueue(QUEUENAME);

	    hiveQueryAllInfo.getHiveQueryInfo().setQueryString(QUERYSTRING);

	    //System.out.println("New Hive Annotation: " + 
	    //hive_annotation.toJson());
	    didUpdateSucceed = dao.putHiveQueryFields(hiveQueryAllInfo);
	}
	    
	if (!didUpdateSucceed)
	    System.out.println("FAILED!");	    
	else System.out.println("SUCCESS!");
    }

    //@Test
	public void dataTransformationMTsugawa() {

	String USERNAME = "mtsugawa";
	String QUEUENAME = "Finance";
	String SUBMITHOST = "bnb-77.62.43.7";
	
	boolean didUpdateSucceed = false; 
        MRJobInfo mrJobInfo = dao.getMRJobInfo("job_201401090205_41581");
        if (mrJobInfo == null)
	    return;

	edu.duke.starfish.workload.storage.model.JobsWithBLOBs
	    jobs = dao.getJobsWithBLOBsByJid(mrJobInfo.getExecId());
	
	//System.out.println("Old Annotation: " + jobs.getAnnotation());
	    
	MRJobInfoAnnotation annotation = mrJobInfo.getAnnotation();
	annotation.setUserName(USERNAME);
	annotation.setQueue(QUEUENAME);
	annotation.setSubmitHost(SUBMITHOST);
	
	String problem = "Excessive wait for resources";
	String[] causes = {"Hadoop Scheduler did not allocate reduce slots to this query for 87 minutes"};
	String[] actions = {"Report the problem to the Hadoop administrator"};
	
	//annotation.addIneff(problem, causes, actions);
	
	//System.out.println("New Annotation: " + mrJobInfo.getAnnotation().toJson());
	didUpdateSucceed = dao.putJobFields(mrJobInfo);
	
	if (!didUpdateSucceed) {
	    System.out.println("FAILED!");	    
	    return;
	}
	
	HiveQueryAllInfo hiveQueryAllInfo =
	    dao.getHiveQueryAllInfo("hue_20140116120505_a6cf83d4-bf03-4d20-9f90-01389c30afcf");
	if (hiveQueryAllInfo != null) {
	    
	    // this will also populate the HiveQueryAnnotation
	    HiveQueryInfoLoader.extractInfoFromJobHistory(hiveQueryAllInfo);
	    HiveQueryAnnotation hive_annotation = 
		hiveQueryAllInfo.getAnnotation();
	    hive_annotation.addIneff(problem, causes, actions);
	    hive_annotation.setUser(USERNAME);	
	    hive_annotation.setQueue(QUEUENAME);	    

	    //System.out.println("New Hive Annotation: " + 
	    //hive_annotation.toJson());
	    didUpdateSucceed = dao.putHiveQueryFields(hiveQueryAllInfo);
	}
	    
	if (!didUpdateSucceed)
	    System.out.println("FAILED!");	    
	else System.out.println("SUCCESS!");
    }

    //@Test 
	public void dataTransformationKarma() {

       String QUERYSTRING = "FROM (SELECT ATTRIBUTES FROM TABLE r JOIN TABLE p ON (r.ip=p.ip) WHERE r.data_date>='20010101' AND r.data_date<='20131213' AND p.data_date='20131213' CLUSTER BY r.uuid ) m INSERT OVERWRITE TABLE TABLE PARTITION (data_date='20131213') REDUCE ATTRIBUTES USING 'java -Xmx4096m -cp ./* UDF' AS (uuid STRING, user_info STRING)";

	String QUEUENAME = "Advertising";
	String SUBMITHOST = "bnb-77.62.43.7";

	String USERNAME = "rajeev";
	String QUERYID = "karma_20140326190909_886c15c8-fddc-4a4d-96f6-f82a19192be6";
	List<String> JOBLIST = new ArrayList<String>();
	JOBLIST.add("job_201403182307_58521");
	JOBLIST.add("job_201403182307_58772");

	String problem1 = "Excessive data spill by map tasks";
	String[] causes1 = {"Output to input data ratio of map tasks is 10x on average"};
	String[] actions1 = {"Set io.sort.mb=940, io.sort.factor=199, and mapred.max.split.size=94371840"};
	
	String problem2 = "Inefficient cluster resource use by reduce tasks";
	String[] causes2 = {"Reduce tasks are started earlier than needed and wait in the shuffle phase for map outputs"};
	String[] actions2 = {"Set mapred.reduce.slowstart.completed.maps=0.65"};
	
	String problem3 = "Invalid map input records";
	String[] causes3 = {"Bad input data"};
	String[] actions3 = {"Check the ETL process"};

	boolean didUpdateSucceed = false; 
	for (String jobId : JOBLIST) {
	    MRJobInfo mrJobInfo = dao.getMRJobInfo(jobId);
	    if (mrJobInfo == null)
		return;
	    
	    //System.out.println("MRJobInfo Name: " + mrJobInfo.getName());
	    //System.out.println("MRJobInfo User: " + mrJobInfo.getUser());
	    
	    edu.duke.starfish.workload.storage.model.JobsWithBLOBs
		jobs = dao.getJobsWithBLOBsByJid(mrJobInfo.getExecId());
	    
	    //System.out.println("Old Annotation: " + jobs.getAnnotation());
	    
	    MRJobInfoAnnotation annotation = mrJobInfo.getAnnotation();
	    annotation.setUserName(USERNAME);
	    //annotation.addIneff(problem1, causes1, actions1);
	    //  annotation.addIneff(problem2, causes2, actions2);
	    annotation.setQueue(QUEUENAME);
	    annotation.setSubmitHost(SUBMITHOST);

	    //System.out.println("New Annotation: " + mrJobInfo.getAnnotation().toJson());
	    didUpdateSucceed = dao.putJobFields(mrJobInfo);
	    
	    if (!didUpdateSucceed) {
		System.out.println("FAILED!");	    
		return;
	    }
	}
	
	didUpdateSucceed = false;
	
	HiveQueryAllInfo hiveQueryAllInfo =
	    dao.getHiveQueryAllInfo(QUERYID);
	if (hiveQueryAllInfo != null) {
	    
	    // this will also populate the HiveQueryAnnotation
	    HiveQueryInfoLoader.extractInfoFromJobHistory(hiveQueryAllInfo);
	    HiveQueryAnnotation hive_annotation = 
		hiveQueryAllInfo.getAnnotation();
	    hive_annotation.addIneff(problem1, causes1, actions1);
	    hive_annotation.addIneff(problem2, causes2, actions2);
	    hive_annotation.addErr(problem3, causes3, actions3);	
	    hive_annotation.setUser(USERNAME);
	    hive_annotation.setQueue(QUEUENAME);
	    
	    hiveQueryAllInfo.getHiveQueryInfo().setQueryString(QUERYSTRING);

	    //System.out.println("New Hive Annotation: " + 
	    //hive_annotation.toJson());
	    didUpdateSucceed = dao.putHiveQueryFields(hiveQueryAllInfo);
	}
	
	if (!didUpdateSucceed)
	    System.out.println("FAILED!");	    
	else System.out.println("SUCCESS!");
    }

    //@Test 
	public void testHealthCheck() {
	
	    //MRJobInfo mrJobInfo = dao.getMRJobInfo("job_201403182307_123106");
	MRJobInfo mrJobInfo = dao.getMRJobInfo("job_201309130805_1098");
	//MRJobInfo mrJobInfo = dao.getMRJobInfo("job_201309130805_1109");

        if (mrJobInfo != null) {
            System.out.println("MRJobInfo Name: " + mrJobInfo.getName());
            System.out.println("MRJobInfo User: " + mrJobInfo.getUser());

	    Set<HadoopParameter> params = new HashSet<HadoopParameter>();
	    for (HadoopParameter param : HadoopParameter.values()) {
		params.add(param);
	    }

	    JobParametersHealthCheck healthCheck = 
		new JobParametersHealthCheck(mrJobInfo, params);
	    
	    Map<HadoopParameter, ParameterCheckResponse> responses = 
		healthCheck.check();
					     
	    for (ParameterCheckResponse response : responses.values()) {
		System.out.println(response);
	    }
	    
	    String io_sort_mb = mrJobInfo.getConf().get("io.sort.mb");
	    System.out.println("io.sort.mb = " + io_sort_mb);
	    String io_sort_rec_perc = mrJobInfo.getConf().get("io.sort.record.percent");
	    System.out.println("io_sort_rec_perc = " + io_sort_rec_perc);
	    String java_opts = mrJobInfo.getConf().get("mapred.child.java.opts");
	    System.out.println("java_opts = " + java_opts);
	}
    }

    //@Test 
	public void testGetWorkflowInfo() {
      
	String[] workflow_instance_id_list = new String[5];

		  
	workflow_instance_id_list[0] = "20140610T050405Z-908684167308192098";
	workflow_instance_id_list[1] = "20140617T050409Z-908690137545454309";
	

	workflow_instance_id_list[0] = "20140506T103517Z-7571454033877908398";
	workflow_instance_id_list[1] = "20140507T163512Z-7571454886940720590"; 
	workflow_instance_id_list[2] = "20140509T203519Z-7571456593438524464"; 
	workflow_instance_id_list[3] = "20140510T213511Z-7571475357069977069"; 
	workflow_instance_id_list[4] = "20140511T133513Z-7571476209130769193"; 

      System.out.println(WorkflowInfo.toJson(workflow_instance_id_list[0], 
					     workflow_instance_id_list));
  } // public void testGetWorkflowInfo() {

    //@Test 
	public void testGetWorkflowInfoRF() {
      
      String[] workflow_instance_id_list = new String[1];
      
      //String WORKFLOW_ID = "20140613T175759Z-4482340140837566645";
      String WORKFLOW_ID = "20140615T080826Z-5532803437670076347";
      
      workflow_instance_id_list[0] = WORKFLOW_ID;

      System.out.println("going to call tojson");
      System.out.println(WorkflowInfo.toJson(WORKFLOW_ID,
					     workflow_instance_id_list));
	} // public void testGetWorkflowInfo() {

    //@Test
    public void testInsertJob() {
	
	List<String> JOBLIST = new ArrayList<String>();
	JOBLIST.add("job_201406011747_0042");
	JOBLIST.add("job_201403182307_58772");
	JOBLIST.add("job_201401090205_74783");
	
	boolean didUpdateSucceed = false; 
	for (String jobId : JOBLIST) {
	    MRJobInfo mrJobInfo = dao.getMRJobInfo(jobId);
	    if (mrJobInfo == null) {
		System.out.println("FAILED!");	    
		continue; 
	    }
	    
	    //System.out.println("MRJobInfo Name: " + mrJobInfo.getName());
	    //System.out.println("MRJobInfo User: " + mrJobInfo.getUser());
	    
	    didUpdateSucceed = dao.putJobFields(mrJobInfo);
	    
	    if (!didUpdateSucceed) 
		System.out.println("FAILED!");	    
	    else System.out.println("SUCCESS!");
	}
    }


    //@Test
    public void testInsertJob1() {

        List<String> JOBLIST = new ArrayList<String>();
	JOBLIST.add("job_201403182307_171718");
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                JOBLIST.add("job_201403182307_228000");
	JOBLIST.add("job_201403182307_239383");
    JOBLIST.add("job_201403182307_250390");
    JOBLIST.add("job_201403182307_209602");
        //JOBLIST.add("job_201403182307_211050");
    //JOBLIST.add("job_201403182307_173064");
      //  JOBLIST.add("job_201403182307_173247");
      //  JOBLIST.add("job_201405200258_328385");
      // JOBLIST.add("job_201405200258_259644");

        //JOBLIST.add("job_201405200258_259644");

        String[] arr = {"job_201405200258_387904"};
        boolean didUpdateSucceed = true;
        for (String jobId : arr) {
            MRJobInfo mrJobInfo = dao.getMRJobInfo(jobId);
            if (mrJobInfo == null) {
                System.out.println("FAILED!");
                continue;
            }

            //System.out.println("MRJobInfo Name: " + mrJobInfo.getName());
            //System.out.println("MRJobInfo User: " + mrJobInfo.getUser());

            // didUpdateSucceed = dao.putJobFields(mrJobInfo);
            System.out.println(jobId);
            JobsWithBLOBs jobsWithBLOBs = dao.getJobsWithBLOBsByJid(jobId);
            String logs = getStringFromCompressedBlob(jobsWithBLOBs.getExtraBlobFieldA());
            ObjectMapper objectMapper = new ObjectMapper();
            try {
                List<MRTaskAttemptLog> list = objectMapper.readValue(logs,
                        TypeFactory.defaultInstance().constructCollectionType(List.class,
                                MRTaskAttemptLog.class));

                System.out.println(list.get(3).getTaskLog());
            } catch (IOException e) {
                e.printStackTrace();
            }

            //String str = mrJobInfo.getAnnotation().toJson();

            //System.out.println(logs);
            if (!didUpdateSucceed)
                System.out.println("FAILED!");
            else System.out.println("SUCCESS!");
        }
    }

  // @Test
    public void testHiveIneff() {
        // done by setUp() //ProfileDBManager dao = new ProfileDBManager("org.postgresql.Driver", url);

        //String QUERYID = "hue_20140708161313_0476e0d7-fe43-4eb0-a14d-7697852b9a9a"; // hue_20140711065858_fff492a0-0437-4e41-8577-d0981c4769ef
        //String QUERYID = "modeling_20140710060101_59889efe-8e81-44f6-8fd2-75b81f755a96";
        //String QUERYID = "modeling_20140708113030_9192cad1-6afd-4b6f-801c-e9a7f6d6065e";
        //String QUERYID = "modeling_20140701032323_b6ee51bf-7c3a-4173-9ee8-7079ec3cb43c";
        String QUERYID = "hdfs_20140715190606_a900ad40-7b16-49b9-b72a-6d59bced5c8d";

        
        http://unravel.rfiserve.net/hive_queries/show_by_exec_id/modeling_20140701032323_b6ee51bf-7c3a-4173-9ee8-7079ec3cb43c    : Test this hive query for 5X blowup factor
         


        HiveQueryAllInfo hiveQueryAllInfo = dao.getHiveQueryAllInfo(QUERYID);
        if (hiveQueryAllInfo != null) {

            // this will also populate the HiveQueryAnnotation
            HiveQueryInfoLoader.extractInfoFromJobHistory(hiveQueryAllInfo);
            HiveQueryAnnotation hive_annotation =
                    hiveQueryAllInfo.getAnnotation();

            System.out.print(hive_annotation.toJson());
            //dao.putHiveQueryFields(hiveQueryAllInfo);

        }

        // done by tearDown() // dao.cleanup();
    }



    //@Test
    public void analyseTablesForTimeRange(){
        	
mysql> select UNIX_TIMESTAMP('2013-05-18 00:00:00');
+---------------------------------------+
| UNIX_TIMESTAMP('2013-05-18 00:00:00') |
+---------------------------------------+
|                            1368860400 |
+---------------------------------------+

mysql> select UNIX_TIMESTAMP('2013-05-21 00:00:00');
+---------------------------------------+
| UNIX_TIMESTAMP('2013-05-21 00:00:00') |
+---------------------------------------+
|                            1369119600 |
+---------------------------------------+
	

        long startTime = 1405029780;
        long endTime = 1405116180;

      //  List<HiveQueryAllInfo> query_list = dao.getHiveQueryAllInfos(startTime, endTime);

        List<String> listQueryIDs = dao.getHiveQueries(startTime, endTime);
        Map<String,Long> totalTableIO = new HashMap<String, Long>();      // <Table_name,TotalRead>
        Map<String,Long> queryIO = new HashMap<String, Long>();      // <Table_name,TotalRead>
        Map<String,Map<String,Long>> queryLocalTableIO = new HashMap<String, Map<String, Long>>();              // <Query_id, localTablesRead>
        Map<String,Long> jobsPerTable = new HashMap<String, Long>();        // <Table_name,TotalRead>
        Map<String,Long> queriesPerTable = new HashMap<String, Long>();     // <Table_name,TotalRead>

        if (query_list != null) {
            for (HiveQueryAllInfo hqa : query_list) {
                System.out.println(" Query ID :" + hqa.getHiveQueryInfo().getQueryId());
                queryLocalTableIO.put(hqa.getHiveQueryInfo().getQueryId(), printTableUsage(hqa, totalTableIO));
            }
        }


        if (listQueryIDs != null) {
            for (String query : listQueryIDs) {
                queryIO =  printTableUsage(query, totalTableIO,queriesPerTable);
                if(queryIO!=null && queryIO.size() != 0)
                    queryLocalTableIO.put(query, queryIO);
            }
        }

        System.out.println("Global Counters ");
        System.out.println("=======================================");
        for(Map.Entry<String,Long> entry : totalTableIO.entrySet()){
            System.out.println(entry.getKey() + " : " + entry.getValue());
        }


        System.out.println("local Counters ");

        for(Map.Entry<String,Map<String,Long>> entry : queryLocalTableIO.entrySet()) {
            System.out.println("\n\nQuery ID : " +entry.getKey());
            System.out.println("=======================================");
            for (Map.Entry<String, Long> entry1 : entry.getValue().entrySet()) {
                System.out.println(entry1.getKey() + " : " + entry1.getValue());
            }
        }

    }



    //@Test
    public static Map<String,Long> printTableUsage(String QUERYID,Map<String,Long> totalTableIO,Map<String,Long> queriesPerTable){


        HiveQueryAllInfo hiveQueryAllInfo = dao.getHiveQueryAllInfo(QUERYID);
        int longestMatch;
        Map<String,String> aliasToPath = null;
        Map<String,Long> tableIO = new HashMap<String, Long>();

        // Global counters as well.
        for(Map.Entry<String,HiveTableInfo> entry : hiveQueryAllInfo.getHiveQueryInfo().getFullTableNameToInfos().entrySet()){
            String fullTableName = entry.getValue().getFullTableName();
            tableIO.put(fullTableName,0l);
            if(totalTableIO.get(fullTableName) == null){
                totalTableIO.put(fullTableName,0l);
            }

            if(queriesPerTable.get(fullTableName) == null){
                queriesPerTable.put(fullTableName,1l);
            }else{
                queriesPerTable.put(fullTableName,queriesPerTable.get(fullTableName) + 1);
            }
        }
        String alias = null;

        for (HiveStageInfo stage : hiveQueryAllInfo.getHiveQueryInfo().getHiveStageInfos()) {
            String jobId = stage.getHadoopJobId();
            if (jobId != null && !jobId.trim().isEmpty()) {

                // MRJobInfo jobInfo = hiveQueryAllInfo.getMrJobManager().getMRJobInfo(jobId);
                MRJobInfo jobInfo = hiveQueryAllInfo.getMRJobInfo(jobId);
                if (jobInfo == null) {
                    continue;
                }


                //System.out.println(" ================================================== ");
                //System.out.println(" Job :" + jobInfo.getExecId());

                aliasToPath = stage.getAliasToPath();

                for (MRMapAttemptInfo attemptInfo : jobInfo.getMapAttempts(MRExecutionStatus.SUCCESS)) {
                    //System.out.println(attemptInfo.getExecId() + "  :   " +attemptInfo.getStateString());

                    longestMatch = Integer.MIN_VALUE;
                    alias = null;
                    String stateStr = attemptInfo.getStateString();
                    if(aliasToPath!= null && stateStr!= null) {
                        for (Map.Entry<String, String> entry : aliasToPath.entrySet()) {  //aliastoPath can be null
                            int matchLength = LPM(entry.getValue(), stateStr);          //statestring can be null
                            if (longestMatch < matchLength) {
                                longestMatch = matchLength;
                                alias = entry.getKey();
                            }
                        }
                    }
                    for(Map.Entry<String,String> entry1 : stage.getAliasToFullTableName().entrySet()){
                        System.out.println(entry1.getKey() + " : " + entry1.getValue());
                    }

                    System.out.println("Alias is : " + alias);
                    if(alias!= null && stage.getAliasToFullTableName()!=null) {
                        String fulltableName = stage.getAliasToFullTableName().get(alias);
                        for(Map.Entry<String,Long> entry1 : tableIO.entrySet()){
                            System.out.println(entry1.getKey() + " : " + entry1.getValue());
                        }

                        if(tableIO!= null && attemptInfo!=null && attemptInfo.getProfile()!=null && tableIO.get(fulltableName)!=null)
                            tableIO.put(fulltableName, tableIO.get(fulltableName) + attemptInfo.getProfile().getCounter(MRCounter.HDFS_BYTES_READ, 0l) +
                                    attemptInfo.getProfile().getCounter(MRCounter.S3_BYTES_READ, 0l));

                        if(totalTableIO!= null && attemptInfo!=null && attemptInfo.getProfile()!=null && totalTableIO.get(fulltableName)!=null)
                            totalTableIO.put(fulltableName, totalTableIO.get(fulltableName) + attemptInfo.getProfile().getCounter(MRCounter.HDFS_BYTES_READ, 0l) +
                                    attemptInfo.getProfile().getCounter(MRCounter.S3_BYTES_READ, 0l));


                    }
                }

            }

        }

        for(Map.Entry<String,Long> entry : tableIO.entrySet()){
            System.out.println(entry.getKey() + " : " + entry.getValue());
        }
        return tableIO;
    }

    public static int LPM(String a, String b) {
        int end = Math.min(a.length(),b.length());
        for (int i = 0; i < end; ++i)
            if (a.charAt(i) != b.charAt(i))
                return i;
        return end;
    }


    //@Test
    public void TestTableIO(){
        long startTime = 1404950400;
        long endTime = 1404950520;
        HiveTableIOInfo.analyseTablesForTimeRange(startTime, endTime);
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


    @Test
    public void testInsertJob2() {
        long startTime = 1404950400;
        long endTime = 1404950520;
        List<String> JOBLIST = null;
        List<Integer> ioSortMB = new ArrayList<Integer>();
        List<String> taskLogList = new ArrayList<String>();

	    JOBLIST = dao.getMRJobInfosByTimeRange(startTime,endTime);

        //String[] arr = {"job_201405200258_387904"};
        boolean didUpdateSucceed = true;
        for (String jobId : JOBLIST) {
            MRJobInfo mrJobInfo = dao.getMRJobInfo(jobId);
            if (mrJobInfo == null) {
                continue;
            }

            ioSortMB.add(mrJobInfo.getConf().getInt(Constants.MR_SORT_MB, Constants.DEF_SORT_MB));
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
                    if(attemptLog.getSpeed().equalsIgnoreCase("slowest") && attemptLog.getStatus().equalsIgnoreCase("success") && attemptLog.getType().equalsIgnoreCase("map"))
                        taskLogList.add(attemptLog.getTaskLog());
                }

            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        System.out.println(Arrays.toString(JOBLIST.toArray()));
        System.out.println(Arrays.toString(ioSortMB.toArray()));
        //EXCEL(JOBLIST,ioSortMB,taskLogList);
    }

} // public class TestProfileDBManager { 
*/