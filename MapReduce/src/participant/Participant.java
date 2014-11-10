package participant;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import fileIO.Partition;
import mapreduce.MRKeyVal;
import mapreduce.Mapper;
import mapreduce.Reducer;
import messages.Command;
import messages.MapAcknowledge;
import messages.MapDone;
import messages.ReduceAcknowledge;
import messages.ReduceDone;
import messages.StopDone;

public class Participant {
	//TODO handle exceptions better!
	private static Map<Integer, Thread> mapThreadsByPid;
	private static Map<Integer, Thread> reduceThreadsByPid;

	public static void main(String[] args) {
		try{
			mapThreadsByPid = new HashMap<Integer, Thread>();
			reduceThreadsByPid = new HashMap<Integer, Thread>();
			
			final ServerSocket masterSocket = new ServerSocket(5050);
			final Socket connection = masterSocket.accept();
			final ObjectOutputStream out = new ObjectOutputStream(connection.getOutputStream());
			ObjectInputStream in = new ObjectInputStream(connection.getInputStream());
			while(true){
				final Command command = (Command) in.readObject();
				if(command.getType().equals("map")){
					Thread mapThread = new Thread(new Runnable(){
						public void run(){
							try {
								MapAcknowledge mapAck = new MapAcknowledge(command.getStringPartitions(), command.getPid());
								if(Thread.interrupted()){
									return;
								}
								out.writeObject(mapAck);
								if(Thread.interrupted()){
									return;
								}
								List<Partition<MRKeyVal>> mappedParts = runMap(command.getStringPartitions(), command.getMapper());
								if(Thread.interrupted()){
									return;
								}
								MapDone mapDone = new MapDone(true, mappedParts, command.getPid());
								out.writeObject(mapDone);
							} catch (IOException e) {
								e.printStackTrace();
							}
						}
					});
					mapThreadsByPid.put(command.getPid(), mapThread);
					mapThread.start();
				}
				else if(command.getType().equals("reduce")){
					Thread reduceThread = new Thread(new Runnable(){
						public void run(){
							try {
								ReduceAcknowledge reduceAck = new ReduceAcknowledge(command.getKeyValPartitions(), command.getPid());
								if(Thread.interrupted()){
									return;
								}
								out.writeObject(reduceAck);
								if(Thread.interrupted()){
									return;
								}
								List<Partition<MRKeyVal>> reducedParts = runReduce(command.getKeyValPartitions(), command.getReducer());
								if(Thread.interrupted()){
									return;
								}
								ReduceDone reduceDone = new ReduceDone(true, reducedParts, command.getPid());
								out.writeObject(reduceDone);
							} catch (IOException e) {
								e.printStackTrace();
							}
						}
					});
					reduceThreadsByPid.put(command.getPid(), reduceThread);
					reduceThread.start();
				}
				else{
					Thread stopThread = new Thread(new Runnable(){
						public void run(){
							stopMap(command.getPid());
							stopReduce(command.getPid());
							StopDone stopDone = new StopDone(true, command.getPid());
							try {
								out.writeObject(stopDone);
							} catch (IOException e) {
								e.printStackTrace();
							}
						}
					});
					stopThread.start();
				}
			}
		} catch(IOException e){
			System.out.println("cannot establish socket");
		} catch (ClassNotFoundException e) {
			System.out.println("cannot identify message class");
		}
	}
	
	public static List<Partition<MRKeyVal>> runMap(List<Partition<String>> partitions, Mapper mapper){
		try {
			return mapper.map(partitions, partitions.get(0).getMaxSize());
		} catch (IOException e) {
			e.printStackTrace();
			return null;
		}
	}
	
	public static void stopMap(int pid){
		if(mapThreadsByPid.containsKey(pid)){
			Thread mapThread = mapThreadsByPid.get(pid);
			if (mapThread.isAlive()){
				mapThread.interrupt();
				mapThreadsByPid.remove(pid);
			}
			else{
				mapThreadsByPid.remove(pid);
			}
		}
	}
	
	public static List<Partition<MRKeyVal>> runReduce(List<Partition<MRKeyVal>> partitions, Reducer reducer){
		try {
			return reducer.reduce(partitions, partitions.get(0).getMaxSize());
		} catch (IOException e) {
			e.printStackTrace();
			return null;
		}
	}
	
	public static void stopReduce(int pid){
		if(reduceThreadsByPid.containsKey(pid)){
			Thread reduceThread = reduceThreadsByPid.get(pid);
			if (reduceThread.isAlive()){
				reduceThread.interrupt();
				reduceThreadsByPid.remove(pid);
			}
			else{
				reduceThreadsByPid.remove(pid);
			}
		}
	}

}
