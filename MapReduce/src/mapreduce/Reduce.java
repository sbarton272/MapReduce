package mapreduce;

import java.util.List;

public interface Reduce {

	public List<MRKeyVal> reduce(List<MRKeyVal> priorKV, MRKeyVal curKV);

}
