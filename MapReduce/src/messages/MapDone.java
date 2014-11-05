package messages;
import fileIO.Partition;

public class MapDone extends Done{

	public MapDone(boolean done, Partition[] parts, int id){
		super(done, parts, id, "map");
	}
	
}
