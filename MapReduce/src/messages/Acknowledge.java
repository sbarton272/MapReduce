package messages;
import fileIO.Partition;

public abstract class Acknowledge extends Message{
	private Partition[] partitions;
	private int pid;
	private String type;
	
	public Acknowledge(Partition[] parts, int id, String t){
		super(parts, id, t);
	}
}
