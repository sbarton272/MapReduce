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
import fileIO.FileServer;
import fileIO.Partition;

public class Participant {
	private static int serverPort = 5050;
	private static Map<Integer, Thread> mapThreadsByPid;
	private static Map<Integer, Thread> reduceThreadsByPid;
	private static int numRestarts = 0;

	public static void main(String[] args) {
		if (args.length == 1) {
			serverPort = Integer.parseInt(args[0]);
		}

		if(numRestarts == 0){
			System.out.println("Starting participant on port " + serverPort);
		}

		try{
			//start file server
			FileServer fileServer = new FileServer();
			fileServer.start();

			mapThreadsByPid = new HashMap<Integer, Thread>();
			reduceThreadsByPid = new HashMap<Integer, Thread>();

			final ServerSocket masterSocket = new ServerSocket(serverPort);
			final Socket connection = masterSocket.accept();
			System.out.println("Accepted master");
			final ObjectOutputStream out = new ObjectOutputStream(connection.getOutputStream());
			ObjectInputStream in = new ObjectInputStream(connection.getInputStream());
			while(true){
				final Command command = (Command) in.readObject();
				if(command.getType().equals("map")){
					System.out.println("Process "+command.getPid()+": Received Map Command");
					Thread mapThread = new Thread(new Runnable(){
						@Override
						public void run(){
							try {
								MapAcknowledge mapAck = new MapAcknowledge(command.getStringPartitions(), command.getPartitionSize(), command.getPid());
								if(Thread.interrupted()){
									return;
								}
								out.writeObject(mapAck);
								if(Thread.interrupted()){
									return;
								}
								ResultPair mapPair = runMap(command.getStringPartitions(), command.getPartitionSize(), command.getMapper());
								if (mapPair.succeeded()){
									List<Partition<MRKeyVal>> mappedParts = mapPair.getPartitions();
									if(Thread.interrupted()){
										return;
									}
									MapDone mapDone = new MapDone(true, mappedParts, command.getPid());
									out.writeObject(mapDone);
									System.out.println("Process "+command.getPid()+": Sent Map Done Message");
								}
								else{
									MapDone mapDone = new MapDone(false, null, command.getPid());
									out.writeObject(mapDone);
									System.out.println("Process "+command.getPid()+": Sent Map Unsuccessful Message");
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
					System.out.println("Process "+redCom.getPid()+": Received Reduce Command");
					Thread reduceThread = new Thread(new Runnable(){
						@Override
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
								ResultPair reducePair = runReduce(redCom.getReduceParts(), redCom.getPartitionSize(), redCom.getReducer());
								if(reducePair.succeeded()){
									List<Partition<MRKeyVal>> reducedParts = reducePair.getPartitions();
									if(Thread.interrupted()){
										return;
									}
									ReduceDone reduceDone = new ReduceDone(true, reducedParts, redCom.getPid());
									out.writeObject(reduceDone);
									System.out.println("Process "+command.getPid()+": Sent Reduce Done Message");
								}
								else{
									ReduceDone reduceDone = new ReduceDone(false, null, redCom.getPid());
									out.writeObject(reduceDone);
									System.out.println("Process "+command.getPid()+": Sent Reduce Unsuccessful Message");
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
					System.out.println("Process "+command.getPid()+": Received Stop Command");
					Thread stopThread = new Thread(new Runnable(){
						@Override
						public void run(){
							stopMap(command.getPid());
							stopReduce(command.getPid());
							StopDone stopDone = new StopDone(true, command.getPid());
							try {
								out.writeObject(stopDone);
								System.out.println("Process "+command.getPid()+": Sent Stop Done Message");
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
			//Restart participant code
			numRestarts++;
			main(args);
		} catch (ClassNotFoundException e) {
			System.out.println("Failure: Participant cannot identify message class, exiting...");
			//Stop participant code; this is a fatal issue
			return;
		}
	}

	public static ResultPair runMap(List<Partition<String>> partitions, int partitionSize, Mapper mapper){
		try {
			return new ResultPair(mapper.map(partitions, partitionSize) ,true);
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

	public static ResultPair runReduce(SortedMap<String,List<Partition<MRKeyVal>>> partitions, int partitionSize, Reducer reducer){
		try {
			return new ResultPair(reducer.reduce(partitions, partitionSize),true);
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
