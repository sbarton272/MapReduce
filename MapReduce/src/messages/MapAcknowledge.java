package messages;
import fileIO.Partition;

public class MapAcknowledge extends Acknowledge{
	
	public MapAcknowledge(Partition[] parts, int id){
		super(parts, id, "map");
	}

}
