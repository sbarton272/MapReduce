package messages;
import java.util.List;
import java.util.SortedMap;

import mapreduce.MRKeyVal;
import fileIO.Partition;

public class ReduceAcknowledge extends Acknowledge{
	private static final long serialVersionUID = 8498501556580752515L;
	SortedMap<String,List<Partition<MRKeyVal>>> partitions;
	
	public ReduceAcknowledge(SortedMap<String,List<Partition<MRKeyVal>>> parts, int id){
		super(id, null, "reduce");
		partitions = parts;
	}
	
	public SortedMap<String,List<Partition<MRKeyVal>>> getReduceParts(){
		return partitions;
	}

}
