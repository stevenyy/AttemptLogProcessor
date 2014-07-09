package test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.StringWriter;
import java.net.MalformedURLException;
import java.net.URL;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.http.HtmlQuoting;

import edu.duke.starfish.profile.profileinfo.execution.mrtaskattempts.MRTaskAttemptInfo;

public class TestCrawlWebpage {

	public static void main(String[] args) {
		URL url;
		InputStream is = null;
		BufferedReader br;
		String line;

		System.out.println("started reading....");

		try {
			url = new URL("http://inw-693.rfiserve.net:50060/tasklog?attemptid=attempt_201405200258_371918_m_000057_0&all=true");
			is = url.openStream();  // throws an IOException
			br = new BufferedReader(new InputStreamReader(is));


			while ((line = br.readLine()) != null) {
				System.out.println(line);
			}

			System.out.println("printing the length of stream: " + is.available());
		} catch (MalformedURLException mue) {
			mue.printStackTrace();
		} catch (IOException ioe) {
			ioe.printStackTrace();
		} finally {
			try {
				if (is != null) is.close();
			} catch (IOException ioe) {
				// nothing to see here
			}
		}
	}

//	public static String fetchLog(MRTaskAttemptInfo attemptInfo) {
		public String fetchLog(String inputString) {
			String retVal = null; 
			assert(inputString != null);
			URL taskAttemptLogUrl = null; 
			try {
				/*			urlString = "http://" + attemptInfo.getTaskTracker().getHostName() 
	    					+ ":" + attemptInfo.getTaskTracker().getPort() +
	    					"/tasklog?attemptid=" + attemptInfo.getExecId();*/
				//System.out.println("URL = " + urlString);
				String urlString = "http://inw-644.rfiserve.net:50060/tasklog?attemptid=" + inputString + "&all=true";
				taskAttemptLogUrl = new URL(urlString);
			} catch (MalformedURLException e) {
				/*			LOG.error("MalformedURLException while fetching URL: " + inputString);
	    			LOG.error("Error: ", e);*/
				System.err.println("MalformedURLException while fetching URL: " + inputString);
				return retVal; 
			}
			assert(taskAttemptLogUrl != null); 
			//System.err.println("taskAttemptLogUrl: " + taskAttemptLogUrl);
			/**
			 * NOTES: 
			 * 1. http://coding.tocea.com/scertify-code/prohibit-url-openstream-uses-rather-urlconnection/
			 *  Using URLConnection is favored over using URL.openStream()
			 *     URLConnection is more powerful and provides important functions such as
			 *     authentication in a easier way.   
			 * 2. Can be speeded up using Java NIO: 
			 *       http://stackoverflow.com/questions/8405062/downloading-files-with-java
			 */
			// Motivated by: 
			//   http://eventuallyconsistent.net/2011/08/02/working-with-urlconnection-and-timeouts/

			InputStream in;
			BufferedReader reader;
			try {
				in = taskAttemptLogUrl.openStream(); // short hand for openConnection().getInputStream
				reader = new BufferedReader(new InputStreamReader(in));
				String inputLine;

				while ((inputLine = HtmlQuoting.unquoteHtmlChars(reader.readLine())) != null) {
					System.out.println(inputLine);
				}
				//	    			retVal = HtmlQuoting.unquoteHtmlChars(IOUtils.toString(in));  // returns all text from webpage once
				IOUtils.closeQuietly(in); 
			} catch (Exception e) {
				/*LOG.error("Exception while reading from URL: " + inputString);*/
				/*LOG.error("Error: ", e);*/
			}
			return retVal; 
			/*		BufferedReader in;
	    		try {
	    			in = new BufferedReader(new InputStreamReader(
	    					taskAttemptLogUrl.openStream()));
	    			String inputLine;

	    			while ((inputLine = in.readLine()) != null) {
	    				System.out.println(inputLine);
	    			}
	    			in.close();
	    		} catch (IOException e) {
	    			e.printStackTrace();
	    		}*/
		} // public String fetchLog(MRTaskAttemptInfo attemptInfo) {
		
		
		// public void fetching the title of website
		
/*	    html = html.replaceAll("\\s+", " ");
	    Pattern p = Pattern.compile("<title>(.*?)</title>");
	    Matcher m = p.matcher(html);
	    while (m.find() == true) {
	      System.out.println(m.group(1));
	    }
*/
	}


