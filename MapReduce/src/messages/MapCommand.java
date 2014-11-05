package messages;
import fileIO.Partition;

public class MapCommand extends Command{
	
	public MapCommand(Partition[] parts, int id){
		super(parts, id, "map");
	}

}
