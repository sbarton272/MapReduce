package mapreduce;

import java.io.Serializable;
import java.util.List;

public interface ReduceFn extends Serializable {

	public MRKeyVal reduce(String key, List<Integer> values);

}
