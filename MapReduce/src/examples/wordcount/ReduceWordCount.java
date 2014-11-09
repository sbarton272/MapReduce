package examples.wordcount;

import java.util.List;

import mapreduce.MRKeyVal;
import mapreduce.Reduce;

public class ReduceWordCount implements Reduce {

	@Override
	public MRKeyVal reduce(String key, List<Integer> values) {
		int sum = 0;
		for (int val : values) {
			sum += val;
		}
		return new MRKeyVal(key, sum);
	}

}
