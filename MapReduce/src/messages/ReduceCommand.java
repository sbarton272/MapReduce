package messages;
import java.util.List;

import mapreduce.MRKeyVal;
import mapreduce.Reducer;
import fileIO.Partition;

public class ReduceCommand extends Command{
	private static final long serialVersionUID = 2388017349451781083L;

	public ReduceCommand(List<Partition<MRKeyVal>> parts, int id, Reducer reduce){
		super(id, parts, "reduce", reduce);
	}

}
