package master;

public class ParticipantDetails {

	private final String hostName;
	private int port;

	public ParticipantDetails(String hostName, int port) {
		this.hostName = hostName;
		this.port = port;
	}

	public ParticipantDetails(String hostName, String port) {
		this(hostName, Integer.parseInt(port));
	}

	public int getPort() {
		return port;
	}

	public String getHostName() {
		return hostName;
	}

	public void setPort(int port) {
		this.port = port;
	}

}
