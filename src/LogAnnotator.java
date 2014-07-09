

public interface LogAnnotator {
	
	// inspiration drawn from this web blog:
	// http://www.mkyong.com/java/how-to-create-xml-file-in-java-dom/
	
	
	public void annotate(String input);
	
	public boolean saveLocal(String input);
	
}
