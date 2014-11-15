package messages;
import java.util.List;

import mapreduce.MRKeyVal;
import mapreduce.Mapper;
import mapreduce.Reducer;
import fileIO.Partition;

public abstract class Command extends Message {
	private static final long serialVersionUID = -7640733688959219774L;
	private Mapper mapper;
	private Reducer reducer;

	public Command(List<Partition<String>> parts, int partSize, int id, String t, Mapper map){
		super(parts, partSize, id, t);
		mapper = map;
	}

	public Command(int id, List<Partition<MRKeyVal>> parts, String t, Reducer reduce){
		super(id, parts, t);
		reducer = reduce;
	}

	public Mapper getMapper(){
		return mapper;
	}

	public Reducer getReducer(){
		return reducer;
	}

}
