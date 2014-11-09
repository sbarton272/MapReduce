package examples.wordcount;

import mapreduce.MRKeyVal;
import mapreduce.Map;

public class MapWordCount implements Map {

	private static final long serialVersionUID = 2301357566813845030L;

	@Override
	public MRKeyVal map(String input) {
		return new MRKeyVal(input.toLowerCase(), 1);
	}

}
