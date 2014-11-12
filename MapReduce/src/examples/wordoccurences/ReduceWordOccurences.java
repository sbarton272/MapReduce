package examples.wordoccurences;

import java.util.List;

import mapreduce.MRKeyVal;
import mapreduce.Reduce;

public class ReduceWordOccurences implements Reduce {

	private static final long serialVersionUID = 7097739604546254388L;

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
