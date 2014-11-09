package messages;
import java.util.List;
import fileIO.Partition;

public class MapCommand extends Command{
	
	public MapCommand(List<Partition<String>> parts, int id){
		super(parts, id, "map");
	}

}
