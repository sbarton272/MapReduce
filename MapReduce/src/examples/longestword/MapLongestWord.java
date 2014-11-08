package examples.longestword;

import mapreduce.MRKeyVal;
import mapreduce.Map;

public class MapLongestWord implements Map {

	@Override
	public MRKeyVal map(String input) {
		return new MRKeyVal(input.toLowerCase(), input.length());
	}

}
