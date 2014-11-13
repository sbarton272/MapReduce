package messages;
import java.util.List;

import mapreduce.MRKeyVal;
import fileIO.Partition;

public class MapDone extends Done{
	private static final long serialVersionUID = 4720839827035681166L;

	public MapDone(boolean done, List<Partition<MRKeyVal>> parts, int id){
		super(done, parts, id, "map");
	}

}
