package examples.wordcount;

import mapreduce.MRKeyVal;
import mapreduce.Map;

public class MapWordCount implements Map {

	@Override
	public MRKeyVal map(String input) {
		return new MRKeyVal(input.toLowerCase(), 1);
	}

}
