package messages;

import fileIO.Partition;

public abstract class Done extends Message{
	private boolean success;
	
	public Done(boolean done, Partition[] parts, int id, String t){
		super(parts, id, t);
		success = done;
	}
	
	public boolean succeeded(){
		return success;
	}

}
