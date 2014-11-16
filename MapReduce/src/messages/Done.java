package messages;

import java.util.List;

import mapreduce.MRKeyVal;
import fileIO.Partition;

public abstract class Done extends Message{
	private static final long serialVersionUID = -8724709875070790365L;
	private final boolean success;

	public Done(boolean done, List<Partition<MRKeyVal>> parts, int id, String t){
		super(id, parts, 0, t);
		success = done;
	}

	public boolean succeeded(){
		return success;
	}

}
