package examples.wordcount;

import mapreduce.MRKeyVal;
import mapreduce.Map;

public class MapWordCount implements Map {

	private static final long serialVersionUID = 2301357566813845030L;

	@Override
	public MRKeyVal map(String input) {
		String word = input.toLowerCase().replaceAll("[^a-z0-9 ]", "");
		if (word.equals("")) {
			return null;
		}
		return new MRKeyVal(word, 1);
	}

}
