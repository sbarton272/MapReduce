package mapreduce;

public interface Map {

	/**
	 * 
	 * Returns null if element is to be ignored
	 * 
	 * @param input
	 * @return
	 */
	public MRKeyVal map(String input);

}
