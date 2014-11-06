package mapreduce;

import java.util.List;

public interface Reduce {

	public MRKeyVal reduce(String key, List<Integer> values);

}
