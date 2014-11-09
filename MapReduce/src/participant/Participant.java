package participant;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.List;

import fileIO.Partition;
import mapreduce.MRKeyVal;
import messages.Command;

public class Participant {

	public static void main(String[] args) {
		try{
			final ServerSocket masterSocket = new ServerSocket(5050);
			final Socket connection = masterSocket.accept();
			ObjectOutputStream out = new ObjectOutputStream(connection.getOutputStream());
			ObjectInputStream in = new ObjectInputStream(connection.getInputStream());
			while(true){
				Command command = (Command) in.readObject();
				if(command.getType().equals("map")){
					runMap(command.getStringPartitions());
				}
				else if(command.getType().equals("reduce")){
					runReduce(command.getKeyValPartitions());
				}
				else{
					stopMap(command.getPid());
					stopReduce(command.getPid());
				}
			}
		} catch(IOException e){
			System.out.println("cannot establish socket");
		} catch (ClassNotFoundException e) {
			System.out.println("cannot identify message class");
		}
	}
	
	public static void runMap(List<Partition<String>> partitions){
		//TODO run map function on instructed partition
		
	}
	
	public static void stopMap(int pid){
		//TODO stop map of partition for pid if running
		
	}
	
	public static void runReduce(List<Partition<MRKeyVal>> partitions){
		//TODO run reduce function on instructed partition
		
	}
	
	public static void stopReduce(int pid){
		//TODO stop reduce of partition for pid if running
		
	}

}
