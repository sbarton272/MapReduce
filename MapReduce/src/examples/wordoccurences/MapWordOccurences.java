package examples.wordoccurences;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import mapreduce.MRKeyVal;
import mapreduce.MapFn;

public class MapWordOccurences implements MapFn {

	private static final long serialVersionUID = 7691009690046665508L;
	private final String word = "macbeth";

	@Override
	public MRKeyVal map(String input) {
		Pattern regex = Pattern.compile(word);
		Matcher matcher = regex.matcher(input.toLowerCase().replaceAll("[^a-z0-9 ]", ""));
		int n = 0;
		while (matcher.find()){
			n +=1;
		}

		MRKeyVal result = null;
		if (n > 0) {
			result = new MRKeyVal(word, n);
		}
		return result;
	}

}
