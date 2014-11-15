package messages;


public class StopCommand extends Command{
	private static final long serialVersionUID = 23758868078862039L;

	public StopCommand(int id){
		super(null, 0, id, "stop", null);
	}

}
