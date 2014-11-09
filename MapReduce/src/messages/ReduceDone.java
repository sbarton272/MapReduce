package messages;
import java.util.List;

import mapreduce.MRKeyVal;
import fileIO.Partition;

public class ReduceDone extends Done{
	
	public ReduceDone(boolean done, List<Partition<MRKeyVal>> parts, int id){
		super(done, parts, id, "reduce");
	}

}
