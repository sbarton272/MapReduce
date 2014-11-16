package messages;
import java.util.List;
import java.util.SortedMap;

import fileIO.Partition;
import mapreduce.MRKeyVal;
import mapreduce.Reducer;

public class ReduceCommand extends Command{
	private static final long serialVersionUID = 2388017349451781083L;
	SortedMap<String,List<Partition<MRKeyVal>>> partitions;
	
	public ReduceCommand(SortedMap<String,List<Partition<MRKeyVal>>> parts, int id, int s, Reducer reduce){
		super(id, null, s, "reduce", reduce);
		partitions = parts;
	}
	
	public SortedMap<String,List<Partition<MRKeyVal>>> getReduceParts(){
		return partitions;
	}

}
