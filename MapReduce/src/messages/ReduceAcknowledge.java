package messages;
import java.util.List;

import mapreduce.MRKeyVal;
import fileIO.Partition;

public class ReduceAcknowledge extends Acknowledge{
	
	public ReduceAcknowledge(List<Partition<MRKeyVal>> parts, int id){
		super(id, parts, "reduce");
	}
	
}
