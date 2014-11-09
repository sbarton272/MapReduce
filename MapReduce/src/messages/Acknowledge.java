package messages;
import java.util.List;

import mapreduce.MRKeyVal;
import fileIO.Partition;

public abstract class Acknowledge extends Message{
	
	public Acknowledge(List<Partition<String>> parts, int id, String t){
		super(parts, id, t);
	}
	
	public Acknowledge(int id, List<Partition<MRKeyVal>> parts, String t){
		super(id, parts, t);
	}
	
}
