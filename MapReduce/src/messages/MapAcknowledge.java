package messages;
import java.util.List;

import fileIO.Partition;

public class MapAcknowledge extends Acknowledge{
	private static final long serialVersionUID = -4994576976490475690L;

	public MapAcknowledge(List<Partition<String>> parts, int id){
		super(parts, id, "map");
	}

}
