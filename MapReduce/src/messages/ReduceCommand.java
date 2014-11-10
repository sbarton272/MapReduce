package messages;
import java.util.List;

import mapreduce.MRKeyVal;
import mapreduce.Reduce;
import fileIO.Partition;

public class ReduceCommand extends Command{

	private static final long serialVersionUID = -8493011701094274054L;
	private final Reduce reduce;

	public ReduceCommand(List<Partition<MRKeyVal>> parts, Reduce reduce, int id){
		super(id, parts, "reduce");
		this.reduce = reduce;
	}

	public Reduce getReduce() {
		return reduce;
	}

}
