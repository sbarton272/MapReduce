package messages;
import java.util.List;

import mapreduce.MRKeyVal;
import fileIO.Partition;

public class ReduceCommand extends Command{
	
	public ReduceCommand(List<Partition<MRKeyVal>> parts, int id){
		super(id, parts, "reduce");
	}

}
