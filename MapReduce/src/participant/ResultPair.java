package participant;

import java.util.List;
import mapreduce.MRKeyVal;
import fileIO.Partition;

public class ResultPair {
	private List<Partition<MRKeyVal>> partitions;
	private boolean success;
	
	public ResultPair(List<Partition<MRKeyVal>> parts, boolean s){
		partitions = parts;
		success = s;
	}
	
	public List<Partition<MRKeyVal>> getPartitions(){
		return partitions;
	}
	
	public boolean succeeded(){
		return success;
	}

}
