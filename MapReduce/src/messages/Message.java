package messages;
import java.io.Serializable;
import java.util.List;
import mapreduce.MRKeyVal;
import fileIO.Partition;

public abstract class Message implements Serializable {
	private List<Partition<String>> stringPartitions;
	private List<Partition<MRKeyVal>> keyValPartitions;
	private int pid;
	private String type;
	
	public Message(List<Partition<String>> parts, int id, String t){
		stringPartitions = parts;
		pid = id;
		type = t;
	}
	
	public Message(int id, List<Partition<MRKeyVal>> parts, String t){
		keyValPartitions = parts;
		pid = id;
		type = t;
	}
	
	public String getType(){
		return type;
	}
	
	public List<Partition<String>> getStringPartitions(){
		return stringPartitions;
	}
	
	public List<Partition<MRKeyVal>> getKeyValPartitions(){
		return keyValPartitions;
	}
	
	public int getPid(){
		return pid;
	}
}