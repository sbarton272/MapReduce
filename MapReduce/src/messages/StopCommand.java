package messages;

import java.util.List;

import mapreduce.MRKeyVal;
import fileIO.Partition;

public class StopCommand extends Command{
	
	public StopCommand(int id){
		super(null, id, "stop", null);
	}

}
