package messages;
import fileIO.Partition;

public class ReduceDone extends Done{
	
	public ReduceDone(boolean done, Partition[] parts, int id){
		super(done, parts, id, "reduce");
	}

}
