package messages;
import java.util.List;

import mapreduce.MRKeyVal;
import fileIO.Partition;

public class ReduceDone extends Done{
	private static final long serialVersionUID = -7373548647828348558L;

	public ReduceDone(boolean done, List<Partition<MRKeyVal>> parts, int id){
		super(done, parts, id, "reduce");
	}

}
