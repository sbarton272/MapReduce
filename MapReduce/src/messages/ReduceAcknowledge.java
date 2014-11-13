package messages;
import java.util.List;

import mapreduce.MRKeyVal;
import fileIO.Partition;

public class ReduceAcknowledge extends Acknowledge{
	private static final long serialVersionUID = 8498501556580752515L;

	public ReduceAcknowledge(List<Partition<MRKeyVal>> parts, int id){
		super(id, parts, "reduce");
	}

}
