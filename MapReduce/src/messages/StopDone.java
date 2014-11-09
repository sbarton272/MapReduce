package messages;
import java.util.List;
import mapreduce.MRKeyVal;
import fileIO.Partition;

public class StopDone extends Done{

	public StopDone(boolean done, int id){
		super(done, null, id, "stop");
	}
	
}
