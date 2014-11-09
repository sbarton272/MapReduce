package messages;
import java.util.List;

import fileIO.Partition;

public class MapAcknowledge extends Acknowledge{
	
	public MapAcknowledge(List<Partition<String>> parts, int id){
		super(parts, id, "map");
	}

}
