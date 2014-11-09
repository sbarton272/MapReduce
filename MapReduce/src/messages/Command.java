package messages;
import java.util.List;

import mapreduce.MRKeyVal;
import fileIO.Partition;

public abstract class Command extends Message {
	
	public Command(List<Partition<String>> parts, int id, String t){
		super(parts, id, t);
	}
	
	public Command(int id, List<Partition<MRKeyVal>> parts, String t){
		super(id, parts, t);
	}
	
}
