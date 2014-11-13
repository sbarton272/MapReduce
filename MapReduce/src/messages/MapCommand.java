package messages;
import java.util.List;

import mapreduce.Mapper;
import fileIO.Partition;

public class MapCommand extends Command{
	private static final long serialVersionUID = 1727122383328968099L;

	public MapCommand(List<Partition<String>> parts, int id, Mapper map){
		super(parts, id, "map", map);
	}

}
