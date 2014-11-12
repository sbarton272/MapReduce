package master;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;

import mapreduce.MRKeyVal;
import mapreduce.Mapper;
import mapreduce.Reducer;
import mergesort.MergeSort;
import messages.MapAcknowledge;
import messages.MapCommand;
import messages.MapDone;
import messages.ReduceAcknowledge;
import messages.ReduceCommand;
import messages.ReduceDone;
import messages.StopCommand;
import messages.StopDone;
import fileIO.Partition;

public class Master {

	private static final String HELP_MSG = "Valid commands are:\n" +
			"start <configFile>\n" +
			"stop <pid>\n" +
			"status <pid>\n";

	private static final String DEFAULT_OUTPUT_DELIM = "-";

	private static Map<String, Integer> participants;
	private static Map<Integer, Integer> numPartsByPid;
	private static Map<Integer, Integer> partsDoneByPid;
	private static Map<Integer, List<Connection>> connectionsByPid;
	private static List<Integer> mapDone;
	private static List<Integer> sortDone;
	private static List<Integer> reduceDone;
	private static List<Integer> writtenToFile;
	private static Mapper mapper;
	private static Reducer reducer;

	public static void main(String[] args) {
		//TODO config loader
		//TODO properly define mapper/reducer based on config file!
		mapper = new Mapper(null);
		reducer = new Reducer(null);

		//TODO handle errors/exceptions more cleanly (many are currently just printing stack trace)

		participants = new HashMap<String, Integer>();
		numPartsByPid = new HashMap<Integer, Integer>();
		partsDoneByPid = new HashMap<Integer, Integer>();
		connectionsByPid = new HashMap<Integer, List<Connection>>();
		mapDone = new ArrayList<Integer>();
		sortDone = new ArrayList<Integer>();
		reduceDone = new ArrayList<Integer>();
		writtenToFile = new ArrayList<Integer>();

		//constantly accept commands from the command line
		final Scanner scanner = new Scanner(System.in);
		Thread commandThread = new Thread(new Runnable() {
			@Override
			public void run() {
				while (true) {
					System.out.println("Enter a command: ");
					final String command = scanner.nextLine();

					//handle a given command
					Thread handleThread = new Thread(new Runnable() {
						@Override
						public void run() {

							int pid = 1;

							//if it's a start command, start mapreduce
							String[] args = command.split("\\s+");
							if ((args.length == 2) && args[0].equals("start")){


								try {
									// Load specified configurations
									final ConfigLoader configLoader = new ConfigLoader(args[1]);

									System.out.println("The PID for this MapReduce process is: "+pid);

									final int threadPid = pid;
									Thread startThread = new Thread(new Runnable() {
										@Override
										public void run() {
											System.out.println("Process "+threadPid+": Starting MapReduce with input file "+configLoader.getInputFile());
											startMapReduce(threadPid, configLoader);
											System.out.println("Process "+threadPid+": MapReduce complete! Results written to "+configLoader.getOutputFile());
										}
									});
									startThread.start();
									pid++;

								} catch (IOException e) {
									System.out.println("Invalid configurations");
								}

							} else if ((args.length == 2) && args[0].equals("status")){
								final int statusPid = Integer.valueOf(args[2]);
								//if invalid pid, tell user to try again
								if (statusPid >= pid){
									System.out.println("The PID you entered is invalid. Please try again.");
								}
								else{
									System.out.println("Process "+statusPid+": Getting status...");
									Thread statusThread = new Thread(new Runnable() {
										@Override
										public void run() {
											getStatus(statusPid);
										}
									});
									statusThread.start();
								}
							} else if ((args.length == 2) && args[0].equals("stop")) {
								final int stopPid = Integer.valueOf(args[1]);
								//if invalid pid, tell user to try again
								if (stopPid >= pid){
									System.out.println("The PID you entered is invalid. Please try again.");
								}
								else{
									System.out.println("Process "+stopPid+" : Stopping MapReduce...");
									Thread stopThread = new Thread(new Runnable() {
										@Override
										public void run() {
											stopMapReduce(stopPid);
											System.out.println("Process "+stopPid+" : MapReduce stopped successfully.");
										}
									});
									stopThread.start();
								}
							}
							else{
								//invalid command
								System.out.println(HELP_MSG);
							}
						}
					});
					handleThread.start();
				}
			}
		});
		commandThread.start();
	}

	public static List<Partition<MRKeyVal>> coordinateMap(final int pid, int partitionSize, List<Connection> connections, String infile){
		final List<Partition<MRKeyVal>> mappedParts = new ArrayList<Partition<MRKeyVal>>();
		final List<Thread> threads = new ArrayList<Thread>();
		try {
			//create partitions
			//TODO use correct input file location/name!
			List<Partition<String>> input = Partition.fileToPartitions(infile, partitionSize);
			//disperse partitions to participants
			int i = 0;
			for (final Connection connection : connections){
				final List<Partition<String>> parts = new ArrayList<Partition<String>>();
				parts.add(input.get(i));
				Thread mapComThread = new Thread(new Runnable() {
					@Override
					public void run() {
						MapCommand mapCom = new MapCommand(parts, pid, mapper);
						try {
							connection.getOutputStream().writeObject(mapCom);
							// TODO handle timeout for acknowledge/done!
							MapAcknowledge mapAck = (MapAcknowledge)connection.getInputStream().readObject();
							MapDone mapDone = (MapDone)connection.getInputStream().readObject();
							mappedParts.addAll(mapDone.getKeyValPartitions());
							if(!mapDone.succeeded()){
								// TODO handle failure of a mapper
							}
						} catch (Exception e) {
							e.printStackTrace();
						}
					}
				});
				threads.add(mapComThread);
				mapComThread.start();
				i++;
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		//TODO retry any failures
		for (Thread thread : threads) {
			try {
				thread.join();
				partsDoneByPid.put(pid, (partsDoneByPid.get(pid)+1));
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		connections = new ArrayList<Connection>();
		return mappedParts;
	}

	public static List<Partition<MRKeyVal>> coordinateReduce(final int pid, List<Partition<MRKeyVal>> partitions, List<Connection> connections){
		try {
			final Partition<MRKeyVal> reducedPart = new Partition<MRKeyVal>(partitions.size());
			final List<Thread> threads = new ArrayList<Thread>();

			// Iterate through all available participants
			for(int i = 0; i < connections.size(); i++){
				final Connection connection = connections.get(i);

				// If out of partitions do not pass any more out
				if(i >= partitions.size()){
					i = connections.size(); // TODO why?
					break;
				}

				// Split partitions among participants
				// TODO THIS IS WHERE I LEFT OFF

				// TODO split by key
				final List<Partition<MRKeyVal>> parts = new ArrayList<Partition<MRKeyVal>>();
				parts.add(partitions.get(i));

				// Send reduce command thread
				Thread reduceComThread = new Thread(new Runnable() {
					@Override
					public void run() {
						ReduceCommand reduceCom = new ReduceCommand(parts, pid, reducer);
						try {
							connection.getOutputStream().writeObject(reduceCom);
							// TODO handle timeout for acknowledge/done!
							ReduceAcknowledge reduceAck = (ReduceAcknowledge)connection.getInputStream().readObject();
							ReduceDone reduceDone = (ReduceDone)connection.getInputStream().readObject();

							// Extract partitions and load all
							reduceDone.getKeyValPartitions();
							// TODO reducedPart

							if(!reduceDone.succeeded()){
								// TODO handle failure of a reducer
							}
						} catch (Exception e) {
							e.printStackTrace();
						}
					}
				});
				threads.add(reduceComThread);
				reduceComThread.start();
			}
			//TODO retry any failures, remove bad connections from list
			for (Thread thread : threads) {
				try {
					thread.join();
					partsDoneByPid.put(pid, (partsDoneByPid.get(pid)+1));
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
			//Perform final reduce, send to good connection
			List<Partition<MRKeyVal>> parts = new ArrayList<Partition<MRKeyVal>>();
			parts.add(reducedPart);
			ReduceCommand reduceCom = new ReduceCommand(parts, pid, reducer);
			Connection connection = connections.get(0);
			try {
				connection.getOutputStream().writeObject(reduceCom);
				// TODO handle timeout for acknowledge/done!
				ReduceAcknowledge reduceAck = (ReduceAcknowledge)connection.getInputStream().readObject();
				ReduceDone reduceDone = (ReduceDone)connection.getInputStream().readObject();
				return reduceDone.getKeyValPartitions();
			} catch (Exception e) {
				e.printStackTrace();
			}
			//TODO retry on another participant if this failed.
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		return null;
	}

	public static List<Connection> connectToParticipants(){
		List<Connection> connections = new ArrayList<Connection>();
		for(String host : participants.keySet()){
			Socket connection;
			try {
				connection = new Socket(host, participants.get(host));
				ObjectOutputStream out = new ObjectOutputStream(connection.getOutputStream());
				ObjectInputStream in = new ObjectInputStream(connection.getInputStream());
				connections.add(new Connection(connection, in, out));
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return connections;
	}

	public static void startMapReduce(int pid, ConfigLoader configLoader){
		try {
			//connect to participants
			List<Connection> connections = connectToParticipants();
			connectionsByPid.put(pid, connections);

			numPartsByPid.put(pid, connections.size());
			partsDoneByPid.put(pid, 0);

			//map
			List<Partition<MRKeyVal>> mappedParts = coordinateMap(pid, configLoader.getPartitionSize(), connections, configLoader.getInputFile().getPath());
			mapDone.add(pid);
			connectionsByPid.remove(pid);

			//sort
			List<Partition<MRKeyVal>> sortedParts = MergeSort.sort(mappedParts, configLoader.getPartitionSize());
			sortDone.add(pid);

			//reconnect to participants
			connections = connectToParticipants();
			connectionsByPid.put(pid, connections);
			numPartsByPid.put(pid, connections.size());
			partsDoneByPid.put(pid, 0);

			//reduce
			List<Partition<MRKeyVal>> reduced = coordinateReduce(pid, sortedParts, connections);
			reduceDone.add(pid);

			//write to output file
			Partition.partitionsToFile(reduced, configLoader.getOutputFile().getPath(), DEFAULT_OUTPUT_DELIM);
			writtenToFile.add(pid);

		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	public static void stopMapReduce(final int pid){
		List<Thread> threads = new ArrayList<Thread>();
		int attempt = 0;
		boolean stopped = false;
		while(attempt < 5){
			if(writtenToFile.contains(pid)){
				System.out.println("Process "+pid+" completed before the command to stop was received.");
				return;
			}
			else if (reduceDone.contains(pid)){
				System.out.println("Process "+pid+" could not cleanly be stopped, it is finishing writing results to the output file.");
				return;
			}
			if (connectionsByPid.containsKey(pid)){
				List<Connection> connections = connectionsByPid.get(pid);
				for (final Connection connection : connections){
					Thread stopThread = new Thread(new Runnable() {
						@Override
						public void run() {
							StopCommand stopCom = new StopCommand(pid);
							try {
								connection.getOutputStream().writeObject(stopCom);
								StopDone done = (StopDone) connection.getInputStream().readObject();
								//TODO check for errors in this; timeout, not done, etc.
							} catch (Exception e) {
								e.printStackTrace();
							}
						}
					});
					threads.add(stopThread);
					stopThread.start();
				}
				for (Thread thread : threads){
					try {
						thread.join();
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
				stopped = true;
				break;
			}
			else{
				try {
					Thread.sleep(5000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				attempt++;
			}
		}
		if (stopped){
			System.out.println("Process "+pid+" has successfully been stopped.");
		}
		else{
			System.out.println("A problem occurred; process "+pid+" could not be stopped.");
		}
	}

	public static void getStatus(int pid){
		//print out what is currently known about the specified process' status
		if(numPartsByPid.containsKey(pid)){
			if(writtenToFile.contains(pid)){
				System.out.println("Process "+pid+" status: the MapReduce process has completed and the result has been written to the specified output file.");
			}
			else if(reduceDone.contains(pid)){
				System.out.println("Process "+pid+" status: the MapReduce process has completed, and the result is currently being written to the output file.");
			}
			else if(sortDone.contains(pid)){
				double percent = (partsDoneByPid.get(pid)/numPartsByPid.get(pid))*100;
				System.out.println("Process "+pid+" status: the map process has completed, the results have been sorted, and the reduce process is "+percent+" percent complete.");
			}
			else if(mapDone.contains(pid)){
				System.out.println("Process "+pid+" status: the map process has completed, the results are currently being sorted to then be reduced.");
			}
			else{
				double percent = (partsDoneByPid.get(pid)/numPartsByPid.get(pid))*100;
				System.out.println("Process "+pid+" status: the map process is "+percent+" percent complete.");
			}
		}
		else{
			System.out.println("Process "+pid+" status: the MapReduce process has not finished reading the data from the input file yet.");
		}
	}

}
