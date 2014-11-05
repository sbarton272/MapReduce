package messages;
import fileIO.Partition;

public class ReduceAcknowledge extends Acknowledge{
	
	public ReduceAcknowledge(Partition[] parts, int id){
		super(parts, id, "reduce");
	}
	
}
