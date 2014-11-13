package messages;

public class StopDone extends Done{
	private static final long serialVersionUID = -5806018419745453205L;

	public StopDone(boolean done, int id){
		super(done, null, id, "stop");
	}

}
