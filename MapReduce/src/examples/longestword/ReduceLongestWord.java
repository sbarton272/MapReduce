package examples.longestword;

import java.util.List;

import mapreduce.MRKeyVal;
import mapreduce.Reduce;

public class ReduceLongestWord implements Reduce{

	@Override
	public MRKeyVal reduce(String key, List<Integer> values) {
		return null;
	}

}
