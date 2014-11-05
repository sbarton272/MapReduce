package messages;
import fileIO.Partition;

public class ReduceCommand extends Command{
	
	public ReduceCommand(Partition[] parts, int id){
		super(parts, id, "reduce");
	}

}
