package mapreduce;

import java.io.Serializable;

public interface Map extends Serializable {

	/**
	 * 
	 * Returns null if element is to be ignored
	 * 
	 * @param input
	 * @return
	 */
	public MRKeyVal map(String input);

}
