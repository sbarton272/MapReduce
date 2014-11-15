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
import java.util.SortedMap;
import java.util.TreeMap;

import mapreduce.MRKeyVal;
import mapreduce.Mapper;
import mapreduce.Reducer;
import messages.MapAcknowledge;
import messages.MapCommand;
import messages.MapDone;
import messages.ReduceAcknowledge;
import messages.ReduceCommand;
import messages.ReduceDone;
import messages.StopCommand;
import messages.StopDone;
import sort.Sort;
import fileIO.FileServer;
import fileIO.Partition;

public class Master {

	private static final String HELP_MSG = "Valid commands are:\n" +
			"start <configFile>\n" +
			"stop <pid>\n" +
			"status <pid>\n";

	private static final String DEFAULT_OUTPUT_DELIM = "-";

	private static List<ParticipantDetails> participants;
	private static Map<Integer, Integer> numPartsByPid;
	private static Map<Integer, Integer> partsDoneByPid;
	private static Map<Integer, List<Connection>> connectionsByPid;
	private static List<Connection> connections;
	private static List<Integer> mapDone;
	private static List<Integer> sortDone;
	private static List<Integer> reduceDone;
	private static List<Integer> writtenToFile;
	private static Mapper mapper;
	private static Reducer reducer;
	private static int mapTimeout;
	private static int reduceTimeout;

	public static void main(String[] args) {
		mapper = new Mapper(null);
		reducer = new Reducer(null);
		mapTimeout = 0;
		reduceTimeout = 0;

		connections = new ArrayList<Connection>();
		participants = new ArrayList<ParticipantDetails>();
		numPartsByPid = new HashMap<Integer, Integer>();
		partsDoneByPid = new HashMap<Integer, Integer>();
		connectionsByPid = new HashMap<Integer, List<Connection>>();
		mapDone = new ArrayList<Integer>();
		sortDone = new ArrayList<Integer>();
		reduceDone = new ArrayList<Integer>();
		writtenToFile = new ArrayList<Integer>();
		
		//start file server
		FileServer fileServer = new FileServer("/tmp");
		fileServer.start();

        // Useful startup messages
        System.out.println("Welcome to MapReduce :)");
        System.out.println(HELP_MSG);

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
									mapper = new Mapper(configLoader.getMapFn());
									reducer = new Reducer(configLoader.getReduceFn());
									mapTimeout = configLoader.getMapTimeoutSec()*1000;
									reduceTimeout = configLoader.getReduceTimeoutSec()*1000;

									System.out.println("The PID for this MapReduce process is: "+pid);

									final int threadPid = pid;
									Thread startThread = new Thread(new Runnable() {
										@Override
										public void run() {
											System.out.println("Process "+threadPid+": Starting MapReduce with input file "+configLoader.getInputFile().getPath());
											boolean done = startMapReduce(threadPid, configLoader);
											if(done){
												String[] brokenPath = configLoader.getOutputFile().getPath().split("\\.");
												String tempPath = brokenPath[0]+"_0"+"."+brokenPath[1];
												System.out.println("Process "+threadPid+": MapReduce complete! Results written to "+configLoader.getNumReducers()+" numbered files starting at "+tempPath);
											}
											else{
												System.out.println("Process "+threadPid+"Error: MapReduce failed.");
											}
										}
									});
									startThread.start();
									pid++;

								} catch (Exception e) {
									System.out.println("Invalid configurations cannot run");
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

	public static List<Partition<MRKeyVal>> coordinateMap(final int pid, List<Connection> connections, List<Partition<String>> input){
		final List<Partition<MRKeyVal>> mappedParts = new ArrayList<Partition<MRKeyVal>>();
		final Map<Connection, Integer> connIdx = new HashMap<Connection, Integer>();
		final Map<Connection, Thread> threadsByConn = new HashMap<Connection, Thread>();
		Map<Integer, List<Partition<String>>> partsByIdx = new HashMap<Integer, List<Partition<String>>>();

		try {
			//disperse partitions to participants
			int j = 0;
			if(connections.size() == 0){
				System.out.println("NO CONNECTIONS");
			}
			for(Partition<String> part : input){
				List<Partition<String>> parts;
				if(partsByIdx.containsKey(j%(connections.size()))){
					parts = partsByIdx.get(j%(connections.size()));
				}
				else{
					parts = new ArrayList<Partition<String>>();
				}
				parts.add(part);
				partsByIdx.put(j%(connections.size()), parts);

				j++;
			}

			//Send map commands to participants
			final List<Partition<String>> failedParts = new ArrayList<Partition<String>>();
			final List<Connection> toRemove = new ArrayList<Connection>();
			//int i = 0;
			//for (final Connection connection : connections){
			for(int i = 0; i < connections.size(); i++){
				final Connection connection = connections.get(i);
				//final List<Partition<String>> parts = new ArrayList<Partition<String>>();
				//parts.add(input.get(i));
				final List<Partition<String>> parts = partsByIdx.get(i);
				Thread mapComThread = new Thread(new Runnable() {
					@Override
					public void run() {
						MapCommand mapCom = new MapCommand(parts, pid, mapper);
						try {
							connection.getOutputStream().writeObject(mapCom);
							MapAcknowledge mapAck = (MapAcknowledge)connection.getInputStream().readObject();
							MapDone mapDone = (MapDone)connection.getInputStream().readObject();
							if(!mapDone.succeeded()){
								//Mapper failed: remove connection from list, store failed partitions, do later
								toRemove.add(connection);
								failedParts.addAll(parts);
							}
							else{
								if(Thread.interrupted()){
									return;
								}
								mappedParts.addAll(mapDone.getKeyValPartitions());
							}
						} catch (Exception e) {
							//Mapper failed: remove connection from list, store failed partitions, do later
							toRemove.add(connection);
							failedParts.addAll(parts);
						}
					}
				});
				threadsByConn.put(connection, mapComThread);
				connIdx.put(connection, i);
				mapComThread.start();
				i++;
			}

			for (Connection conn : connections) {
				Thread thread = threadsByConn.get(conn);
				try {
					thread.join(mapTimeout);

					if (thread.isAlive()){
						//Mapper timed out; interrupt thread, remove connection, store failed partitions
						thread.interrupt();
						toRemove.add(conn);
						failedParts.addAll(partsByIdx.get(connIdx.get(conn)));
					}
					else{
						partsDoneByPid.put(pid, (partsDoneByPid.get(pid)+1));
					}
				} catch (InterruptedException e) {
					System.out.println("Process "+pid+" was interrupted while waiting for results from map participants.");
				}
			}

			//Retry any failures, remove bad connections from list
			if(!toRemove.isEmpty()){
				for(Connection connection : toRemove){
					connections.remove(connection);
				}
				if(!failedParts.isEmpty()){
					List<Partition<MRKeyVal>> retriedResults = coordinateMap(pid, connections, failedParts);
					mappedParts.addAll(retriedResults);
				}
			}

			connections = new ArrayList<Connection>();
			return mappedParts;
		} catch (Exception e) {
			//All expected possible issues are handled above, this is a catch-all for any unexpected issues;
			//it just prints out the stack trace so you can debug because this would only be an odd issue
			e.printStackTrace();
		}

		return null;
	}

	public static boolean coordinateReduce(final int pid, SortedMap<String, List<Partition<MRKeyVal>>> sortedParts, List<Connection> connections, final ConfigLoader configLoader){
		boolean success = true;
		try {
			System.out.println("config num reducers: "+configLoader.getNumReducers());
			System.out.println("num connections: "+connections.size());
			int numReducers = Math.min(configLoader.getNumReducers(), connections.size());
			if(numReducers == 0){
				System.out.println("Process "+pid+" Error: No reducers.");
				return false;
			}
			final Map<Connection, Thread> threadsByConn = new HashMap<Connection, Thread>();
			final Map<Connection, Integer> connIdx = new HashMap<Connection, Integer>();
			final Map<Integer, SortedMap<String,List<Partition<MRKeyVal>>>> partsByIdx = new HashMap<Integer, SortedMap<String,List<Partition<MRKeyVal>>>>();

			// Iterate through all partitions, distribute to participants as evenly as possible
			int i = 0;
			for(String key : sortedParts.keySet()){
				SortedMap<String,List<Partition<MRKeyVal>>> storedSort;
				if(partsByIdx.containsKey(i%numReducers)){
					storedSort = partsByIdx.get(i%numReducers);
				}
				else{
					storedSort = new TreeMap<String,List<Partition<MRKeyVal>>>();
				}
				storedSort.put(key, sortedParts.get(key));
				partsByIdx.put(i%numReducers, storedSort);

				i++;
			}

			//Send participants reduce commands with partitions defined above
			final SortedMap<String, List<Partition<MRKeyVal>>> failedParts = new TreeMap<String, List<Partition<MRKeyVal>>>();
			final List<Connection> toRemove = new ArrayList<Connection>();
			for(int j = 0; j < numReducers; j++){
				if(!partsByIdx.containsKey(j)){
					break;
				}
				else{
					final Connection connection = connections.get(j);
					final SortedMap<String,List<Partition<MRKeyVal>>> parts = partsByIdx.get(j);
					final int tempJ = j;
					//Sends participant j a reduce command, handles results
					Thread reduceComThread = new Thread(new Runnable() {
						@Override
						public void run() {
							ReduceCommand reduceCom = new ReduceCommand(parts, pid, reducer);
							try {
								connection.getOutputStream().writeObject(reduceCom);

								ReduceAcknowledge reduceAck = (ReduceAcknowledge)connection.getInputStream().readObject();
								ReduceDone reduceDone = (ReduceDone)connection.getInputStream().readObject();

								if(!reduceDone.succeeded()){
									//Reducer failed: remove connection from list, store failed partitions, do later
									toRemove.add(connection);
									failedParts.putAll(parts);
								}
								else{
									// Extract partitions and load all
									List<Partition<MRKeyVal>> reduced = reduceDone.getKeyValPartitions();
									if(Thread.interrupted()){
										return;
									}
									String[] brokenPath = configLoader.getOutputFile().getPath().split("\\.");
									String tempPath = brokenPath[0]+"_"+tempJ+"."+brokenPath[1];
									Partition.partitionsToFile(reduced, tempPath, DEFAULT_OUTPUT_DELIM);
								}
							} catch (Exception e) {
								//Reducer failed somewhere, remove connection from list, store failed partitions
								System.out.println("RANDOM REDUCE EXCEPTION");
								e.printStackTrace();
								toRemove.add(connection);
								failedParts.putAll(parts);
							}
						}
					});
					threadsByConn.put(connection, reduceComThread);
					connIdx.put(connection, j);
					reduceComThread.start();
				}
			}

			//Join all threads
			for (Connection conn : threadsByConn.keySet()){
				Thread thread = threadsByConn.get(conn);
				int m = connIdx.get(conn);
				try {
					thread.join(reduceTimeout);

					//if thread is still alive after timeout period, interrupt thread and add partitions to retries
					if(thread.isAlive()){
						System.out.println("REDUCE TIMEOUT");
						thread.interrupt();
						toRemove.add(conn);
						SortedMap<String,List<Partition<MRKeyVal>>> parts = partsByIdx.get(m);
						failedParts.putAll(parts);
					}
					else{
						partsDoneByPid.put(pid, (partsDoneByPid.get(pid)+1));
					}
				} catch (InterruptedException e) {
					System.out.println("Process "+pid+" was interrupted while waiting for results from reduceparticipants.");
				}
			}
			//Retry any failures, remove bad connections from list
			if(!toRemove.isEmpty()){
				for(Connection connection : toRemove){
					connections.remove(connection);
				}
				if(!failedParts.isEmpty()){
					success = coordinateReduce(pid, failedParts, connections, configLoader);
				}
			}

		} catch (Exception e1) {
			//All expected possible issues are handled above, this is a catch-all for any unexpected issues;
			//it just prints out the stack trace so you can debug because this would only be an odd issue
			e1.printStackTrace();
			success = false;
		}
		return success;
	}

	public static List<Connection> connectToParticipants(){
		if(connections.isEmpty()){
			List<Connection> conns = new ArrayList<Connection>();
			System.out.println("num participants: "+participants.size());
			for(ParticipantDetails participant : participants){
				String host = participant.getHostName();
				int port = participant.getPort();
				System.out.println("try: "+host);
				Socket connection;
				try {
					connection = new Socket(host, port);
					ObjectOutputStream out = new ObjectOutputStream(connection.getOutputStream());
					ObjectInputStream in = new ObjectInputStream(connection.getInputStream());
					conns.add(new Connection(connection, in, out));
				} catch (IOException e) {
					System.out.println("Master failed to connect to the following participant: "+host);
				}
			}
			connections = conns;
			System.out.println("num connections = "+connections.size());
		}
		else{
			System.out.println("remove bad connections, currently "+connections.size());
			for(Connection connection : connections){
				if(!connection.getSocket().isConnected()){
					connections.remove(connection);
				}
			}
			System.out.println("num remaining: "+connections.size());
		}
		return connections;
	}

	public static boolean startMapReduce(int pid, ConfigLoader configLoader){
		try {
			participants = configLoader.getParticipants();
			//connect to participants
			System.out.println("connecting 1");
			List<Connection> connections = connectToParticipants();
			if(connections.size() == 0){
				System.out.println("No participants are connected.");
				return false;
			}
			connectionsByPid.put(pid, connections);

			numPartsByPid.put(pid, connections.size());
			partsDoneByPid.put(pid, 0);

			//map
			System.out.println("mapping");
			List<Partition<String>> input = Partition.fileToPartitions(configLoader.getInputFile().getPath(), configLoader.getPartitionSize());
			System.out.println("sending map "+connections.size()+" connections");
			List<Partition<MRKeyVal>> mappedParts = coordinateMap(pid, connections, input);
			if(mappedParts.equals(null)){
				System.out.println("Process "+pid+" Error: Map process failed, aborting...");
				return false;
			}
			mapDone.add(pid);
			connectionsByPid.remove(pid);

			//sort
			System.out.println("sorting");
			SortedMap<String,List<Partition<MRKeyVal>>> sortedParts = Sort.sort(mappedParts, configLoader.getPartitionSize());
			sortDone.add(pid);

			//reconnect to participants
			System.out.println("connecting 2");
			connections = connectToParticipants();
			if(connections.size() == 0){
				System.out.println("No participants are connected.");
				return false;
			}
			connectionsByPid.put(pid, connections);
			numPartsByPid.put(pid, connections.size());
			partsDoneByPid.put(pid, 0);

			//reduce
			System.out.println("reducing");
			boolean reduced = coordinateReduce(pid, sortedParts, connections, configLoader);
			if (reduced){
				reduceDone.add(pid);
				writtenToFile.add(pid);
			}
			else{
				System.out.println("Process "+pid+" Error: the reduce process failed to successfully reduce and write to the output files.");
				return false;
			}

            // Clean-up input partitions
            Partition.deleteAll(input);

			return true;

		} catch (Exception e) {
			//All expected possible issues are handled above, this is a catch-all for any unexpected issues;
			//it just prints out the stack trace so you can debug because this would only be an odd issue
			e.printStackTrace();
			return false;
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
				final List<Connection> failures = new ArrayList<Connection>();
				for (final Connection connection : connections){
					Thread stopThread = new Thread(new Runnable() {
						@Override
						public void run() {
							StopCommand stopCom = new StopCommand(pid);
							try {
								connection.getOutputStream().writeObject(stopCom);
								StopDone done = (StopDone) connection.getInputStream().readObject();
								if(!done.succeeded()){
									System.out.println("Process "+pid+" Error: Stopping failed on a participant.");
									failures.add(connection);
								}
							} catch (Exception e) {
								//An error occurred with the connection to the participant, move on.
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
						System.out.println("Process "+pid+" Error: the thread to stop this process was interrupted while waiting for stop threads to finish.");
					}
				}
				if(failures.isEmpty()){
					stopped = true;
				}
				else{
					stopped = false;
				}
				break;
			}
			else{
				try {
					Thread.sleep(5000);
				} catch (InterruptedException e) {
					System.out.println("Process "+pid+" Error: the stop thread was interrupted while waiting for a clean stopping point");
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
