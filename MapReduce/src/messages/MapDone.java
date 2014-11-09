package messages;
import java.util.List;

import mapreduce.MRKeyVal;
import fileIO.Partition;

public class MapDone extends Done{

	public MapDone(boolean done, List<Partition<MRKeyVal>> parts, int id){
		super(done, parts, id, "map");
	}
	
}
