package examples.wordoccurances;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import mapreduce.MRKeyVal;
import mapreduce.Map;

public class MapWordOccurances implements Map {

	private final String word = "the";

	@Override
	public MRKeyVal map(String input) {
		Pattern regex = Pattern.compile(word);
		Matcher matcher = regex.matcher(input);
		int n = 0;
		while (matcher.find()){
			n +=1;
		}
		return new MRKeyVal(word, n);
	}

}
