package messages;
import fileIO.Partition;

public abstract class Command extends Message {
	
	public Command(Partition[] parts, int id, String t){
		super(parts, id, t);
	}
	
}
