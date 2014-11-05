package messages;
import java.io.Serializable;
import fileIO.Partition;

public abstract class Message implements Serializable {
	private Partition[] partitions;
	private int pid;
	private String type;
	
	public Message(Partition[] parts, int id, String t){
		partitions = parts;
		pid = id;
		type = t;
	}
	
	public String getType(){
		return type;
	}
	
	public Partition[] getPartitions(){
		return partitions;
	}
	
	public int getPid(){
		return pid;
	}
}