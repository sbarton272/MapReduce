package examples.wordcount;

import java.util.List;

import mapreduce.MRKeyVal;
import mapreduce.ReduceFn;

public class ReduceWordCount implements ReduceFn {

	private static final long serialVersionUID = -9134448523192868244L;

	@Override
	public MRKeyVal reduce(String key, List<Integer> values) {
		int sum = 0;
		for (int val : values) {
			sum += val;
		}
		return new MRKeyVal(key, sum);
	}

}
