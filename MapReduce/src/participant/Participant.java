package participant;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import fileIO.Partition;
import mapreduce.MRKeyVal;
import mapreduce.Mapper;
import mapreduce.Reducer;
import messages.Command;
import messages.MapAcknowledge;
import messages.MapDone;
import messages.ReduceAcknowledge;
import messages.ReduceCommand;
import messages.ReduceDone;
import messages.StopDone;

public class Participant {
	private static Map<Integer, Thread> mapThreadsByPid;
	private static Map<Integer, Thread> reduceThreadsByPid;

	public static void main(String[] args) {
		try{
			mapThreadsByPid = new HashMap<Integer, Thread>();
			reduceThreadsByPid = new HashMap<Integer, Thread>();
			
			final ServerSocket masterSocket = new ServerSocket(5050);
			final Socket connection = masterSocket.accept();
			System.out.println("accepted master");
			final ObjectOutputStream out = new ObjectOutputStream(connection.getOutputStream());
			ObjectInputStream in = new ObjectInputStream(connection.getInputStream());
			while(true){
				final Command command = (Command) in.readObject();
				System.out.println("got command: "+command.getType());
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
								ResultPair mapPair = runMap(command.getStringPartitions(), command.getMapper());
								if (mapPair.succeeded()){
									List<Partition<MRKeyVal>> mappedParts = mapPair.getPartitions();
									if(Thread.interrupted()){
										return;
									}
									MapDone mapDone = new MapDone(true, mappedParts, command.getPid());
									out.writeObject(mapDone);
								}
								else{
									MapDone mapDone = new MapDone(false, null, command.getPid());
									out.writeObject(mapDone);
								}
							} catch (IOException e) {
								//Cannot successfully write objects to master, exit this thread
								return;
							}
						}
					});
					mapThreadsByPid.put(command.getPid(), mapThread);
					mapThread.start();
				}
				else if(command.getType().equals("reduce")){
					final ReduceCommand redCom = (ReduceCommand) command;
					Thread reduceThread = new Thread(new Runnable(){
						public void run(){
							try {
								ReduceAcknowledge reduceAck = new ReduceAcknowledge(redCom.getReduceParts(), redCom.getPid());
								if(Thread.interrupted()){
									return;
								}
								out.writeObject(reduceAck);
								if(Thread.interrupted()){
									return;
								}
								ResultPair reducePair = runReduce(redCom.getReduceParts(), redCom.getReducer());
								if(reducePair.succeeded()){
									List<Partition<MRKeyVal>> reducedParts = reducePair.getPartitions();
									if(Thread.interrupted()){
										return;
									}
									ReduceDone reduceDone = new ReduceDone(true, reducedParts, redCom.getPid());
									out.writeObject(reduceDone);
								}
								else{
									ReduceDone reduceDone = new ReduceDone(false, null, redCom.getPid());
									out.writeObject(reduceDone);
								}
							} catch (IOException e) {
								//Cannot successfully write objects to master, exit this thread
								return;
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
								//Cannot successfully write objects to master, exit this thread
								return;
							}
						}
					});
					stopThread.start();
				}
			}
		} catch(IOException e){
			System.out.println("Participant cannot establish socket");
			e.printStackTrace();
			//Stop participant code; this is a fatal issue
			return;
		} catch (ClassNotFoundException e) {
			System.out.println("Participant cannot identify message class");
			//Stop participant code; this is a fatal issue
			return;
		}
	}
	
	public static ResultPair runMap(List<Partition<String>> partitions, Mapper mapper){
		try {
			return new ResultPair(mapper.map(partitions, partitions.get(0).getMaxSize()),true);
		} catch (IOException e) {
			//Map failed, return appropriate values
			return new ResultPair(null, false);
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
	
	public static ResultPair runReduce(SortedMap<String,List<Partition<MRKeyVal>>> partitions, Reducer reducer){
		try {
			String tempKey = (String) partitions.keySet().toArray()[0];
			return new ResultPair(reducer.reduce(partitions, partitions.get(tempKey).get(0).getMaxSize()),true);
		} catch (IOException e) {
			//Reduce failed, return appropriate values
			return new ResultPair(null, false);
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
