package messages;
import java.util.List;

import mapreduce.MRKeyVal;
import fileIO.Partition;

public abstract class Acknowledge extends Message{

	private static final long serialVersionUID = -1176895048450821188L;

	public Acknowledge(List<Partition<String>> parts, int partSize, int id, String t){
		super(parts, partSize, id, t);
	}

	public Acknowledge(int id, List<Partition<MRKeyVal>> parts, String t){
		super(id, parts, 0, t);
	}

}
