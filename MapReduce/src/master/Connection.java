package master;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;

public class Connection {
	private Socket socket;
	private ObjectInputStream in;
	private ObjectOutputStream out;
	
	public Connection(Socket s, ObjectInputStream i, ObjectOutputStream o){
		socket = s;
		in = i;
		out = o;
	}
	
	public Socket getSocket(){
		return socket;
	}
	
	public ObjectInputStream getInputStream(){
		return in;
	}
	
	public ObjectOutputStream getOutputStream(){
		return out;
	}

}
