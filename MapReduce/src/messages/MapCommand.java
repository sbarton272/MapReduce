package messages;
import java.util.List;

import mapreduce.Map;
import fileIO.Partition;

public class MapCommand extends Command {

	private static final long serialVersionUID = 1287220154469997008L;
	private final Map map;

	public MapCommand(List<Partition<String>> parts, Map map, int id){
		// TODO pass the map object too
		super(parts, id, "map");
		this.map = map;
	}

	public Map getMap() {
		return map;
	}

}
