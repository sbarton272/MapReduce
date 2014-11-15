package examples.wordcount;

import mapreduce.MRKeyVal;
import mapreduce.MapFn;

public class MapWordCount implements MapFn {

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
