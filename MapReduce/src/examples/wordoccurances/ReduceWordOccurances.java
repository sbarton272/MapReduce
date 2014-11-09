package examples.wordoccurances;

import java.util.List;

import mapreduce.MRKeyVal;
import mapreduce.Reduce;

public class ReduceWordOccurances implements Reduce {

	@Override
	public MRKeyVal reduce(String key, List<Integer> values) {

		// Sum all values
		int sum = 0;
		for(int val : values) {
			sum += val;
		}
		return new MRKeyVal(key,sum);
	}

}
