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
import participant.ResultPair;
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
	private static Map<Integer, Boolean> stopByPid;
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
		stopByPid = new HashMap<Integer, Boolean>();
		mapDone = new ArrayList<Integer>();
		sortDone = new ArrayList<Integer>();
		reduceDone = new ArrayList<Integer>();
		writtenToFile = new ArrayList<Integer>();

		//start file server
		FileServer fileServer;
		try {
			fileServer = new FileServer();
			fileServer.start();
		} catch (Exception e1) {
			System.out.println("FileIO requires a config file");
		}

		// Useful startup messages
		System.out.println("Welcome to MapReduce :)");
		System.out.println(HELP_MSG);

		//constantly accept commands from the command line
		final Scanner scanner = new Scanner(System.in);
		Thread commandThread = new Thread(new Runnable() {
			@Override
			public void run() {
				int pid = 1;

				System.out.println("Enter a command: ");
				while (true) {
					final String command = scanner.nextLine();

					final int threadPid = pid;
					pid++;

					//handle a given command
					Thread handleThread = new Thread(new Runnable() {
						@Override
						public void run() {

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
									participants = configLoader.getParticipants();

									System.out.println("The PID for this MapReduce process is: "+threadPid);

									Thread startThread = new Thread(new Runnable() {
										@Override
										public void run() {
											System.out.println("Process "+threadPid+": Starting MapReduce with input file "+configLoader.getInputFile().getPath());
											stopByPid.put(threadPid, false);
											boolean done = startMapReduce(threadPid, configLoader);
											if(done){
												String[] brokenPath = configLoader.getOutputFile().getPath().split("\\.");
												String tempPath = brokenPath[0]+"_0"+"."+brokenPath[1];
												System.out.println("Process "+threadPid+": MapReduce complete! Results written to "+connections.size()+" numbered files starting at "+tempPath);
											}
										}
									});
									startThread.start();

								} catch (Exception e) {
									System.out.println("Invalid configurations cannot run");
								}

							} else if ((args.length == 2) && args[0].equals("status")){
								final int statusPid = Integer.valueOf(args[1]);
								//if invalid pid, tell user to try again
								if (statusPid >= threadPid){
									System.out.println("Status Request: The PID you entered is invalid. Please try again.");
								}
								else{
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
								if (stopPid >= threadPid){
									System.out.println("Stop Request: The PID you entered is invalid. Please try again.");
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

	public static List<Partition<MRKeyVal>> coordinateMap(final int pid, List<Connection> connections, List<Partition<String>> input, final int partitionSize){
		final List<Partition<MRKeyVal>> mappedParts = new ArrayList<Partition<MRKeyVal>>();
		final Map<Connection, Integer> connIdx = new HashMap<Connection, Integer>();
		final Map<Connection, Thread> threadsByConn = new HashMap<Connection, Thread>();
		Map<Integer, List<Partition<String>>> partsByIdx = new HashMap<Integer, List<Partition<String>>>();

		try {
			//disperse partitions to participants
			int j = 0;
			if(connections.size() == 0){
				System.out.println("Process "+pid+" Error: No Mappers are connected.");
				return new ArrayList<Partition<MRKeyVal>>();
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

			for(int i = 0; i < connections.size(); i++){
				final Connection connection = connections.get(i);
				final List<Partition<String>> parts = partsByIdx.get(i);
				Thread mapComThread = new Thread(new Runnable() {
					@Override
					public void run() {
						MapCommand mapCom = new MapCommand(parts, partitionSize, pid, mapper);
						try {
							connection.getOutputStream().flush();
							connection.getOutputStream().writeObject(mapCom);
							MapAcknowledge mapAck = (MapAcknowledge)connection.getInputStream().readObject();
							MapDone mapDone = (MapDone)connection.getInputStream().readObject();
							if(!mapDone.succeeded()){
								//Mapper failed: remove connection from list, store failed partitions, do later
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
			}

			for (Connection conn : connections) {
				if (threadsByConn.containsKey(conn)){
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
					}
				}
			}

			//Retry any failures, remove bad connections from list
			if(!failedParts.isEmpty()){
				System.out.println("Process "+pid+" : Retrying failed maps...");
				for(Connection connection : toRemove){
					connections.remove(connection);
				}
				if(!failedParts.isEmpty()){
					List<Partition<MRKeyVal>> retriedResults = coordinateMap(pid, connections, failedParts, partitionSize);
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

		return new ArrayList<Partition<MRKeyVal>>();
	}

	public static boolean coordinateReduce(final int pid, SortedMap<String, List<Partition<MRKeyVal>>> sortedParts, List<Connection> connections, final int partitionSize, final ConfigLoader configLoader){
		boolean success = true;
		try {
			int numReducers = Math.min(configLoader.getNumReducers(), connections.size());
			if(numReducers == 0){
				System.out.println("Process "+pid+" Error: No reducers.");
				return false;
			}
			final Map<Connection, Thread> threadsByConn = new HashMap<Connection, Thread>();
			final Map<Connection, Integer> connIdx = new HashMap<Connection, Integer>();
			final Map<Integer, SortedMap<String,List<Partition<MRKeyVal>>>> partsByIdx = new HashMap<Integer, SortedMap<String,List<Partition<MRKeyVal>>>>();

			// Iterate through all partitions, distribute to participants as evenly as possible
			int partsPerReducer;
			if(sortedParts.size() <= numReducers){
				partsPerReducer = 1;
			}
			else if((sortedParts.size()%numReducers) == 0){
				partsPerReducer = sortedParts.size()/numReducers;
			}
			else{
				partsPerReducer = (sortedParts.size()/numReducers)+1;
			}
			int counter = 1;
			int connectionNum = 0;
			for(String key : sortedParts.keySet()){
				SortedMap<String,List<Partition<MRKeyVal>>> storedSort;
				if(partsByIdx.containsKey(connectionNum)){
					storedSort = partsByIdx.get(connectionNum);
				}
				else{
					storedSort = new TreeMap<String,List<Partition<MRKeyVal>>>();
				}
				storedSort.put(key, sortedParts.get(key));
				partsByIdx.put(connectionNum, storedSort);

				if(counter == partsPerReducer){
					counter = 1;
					connectionNum++;
				}
				else{
					counter++;
				}
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
							ReduceCommand reduceCom = new ReduceCommand(parts, pid, partitionSize, reducer);
							try {
								connection.getOutputStream().flush();
								connection.getOutputStream().writeObject(reduceCom);

								ReduceAcknowledge reduceAck = (ReduceAcknowledge)connection.getInputStream().readObject();
								ReduceDone reduceDone = (ReduceDone)connection.getInputStream().readObject();

								if(!reduceDone.succeeded()){
									//Reducer failed: remove connection from list, store failed partitions, do later
									//toRemove.add(connection);
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
									if(!stopByPid.get(pid)){
										Partition.partitionsToFile(reduced, tempPath, DEFAULT_OUTPUT_DELIM);
									}
								}
							} catch (Exception e) {
								//Reducer failed somewhere, remove connection from list, store failed partitions
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
						thread.interrupt();
						toRemove.add(conn);
						SortedMap<String,List<Partition<MRKeyVal>>> parts = partsByIdx.get(m);
						failedParts.putAll(parts);
					}
					else{
						partsDoneByPid.put(pid, (partsDoneByPid.get(pid)+1));
					}
				} catch (InterruptedException e) {
				}
			}
			//Retry any failures, remove bad connections from list
			if(!toRemove.isEmpty()){
				System.out.println("Process "+pid+" : Retrying failed reduces...");
				for(Connection connection : toRemove){
					connections.remove(connection);
				}
				if(!failedParts.isEmpty()){
					success = coordinateReduce(pid, failedParts, connections, partitionSize, configLoader);
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

	public static synchronized List<Connection> connectToParticipants(int pid){
		if(connections.isEmpty()){
			List<Connection> conns = new ArrayList<Connection>();
			for(ParticipantDetails participant : participants){
				String host = participant.getHostName();
				int port = participant.getPort();
				Socket connection;
				try {
					connection = new Socket(host, port);
					ObjectOutputStream out = new ObjectOutputStream(connection.getOutputStream());
					ObjectInputStream in = new ObjectInputStream(connection.getInputStream());
					conns.add(new Connection(connection, in, out));
				} catch (IOException e) {
					System.out.println("All Processes: Master could not connect to "+host+", so that participant will not be used for any process.");
				}
			}
			connections = conns;
		}
		else{
			for(Connection connection : connections){
				if(!connection.getSocket().isConnected()){
					connections.remove(connection);
				}
			}
		}

		connectionsByPid.put(pid, connections);
		numPartsByPid.put(pid, connections.size());
		partsDoneByPid.put(pid, 0);

		return connections;
	}

	public static synchronized ResultPair startMapOrReduce(String type, int pid, List<Connection> connections, List<Partition<String>> stringIn, int partSize, ConfigLoader loader, SortedMap<String,List<Partition<MRKeyVal>>> sortedParts){
		if(type.equals("map")){
			return new ResultPair(coordinateMap(pid, connections, stringIn, partSize), true);
		}
		else{
			boolean succeeded = coordinateReduce(pid, sortedParts, connections, partSize, loader);
			if(succeeded){
				reduceDone.add(pid);
				writtenToFile.add(pid);
			}
			return new ResultPair(null, succeeded);
		}
	}

	public static boolean startMapReduce(int pid, ConfigLoader configLoader){
		try {
			//connect to participants
			if(stopByPid.get(pid)){
				return false;
			}
			List<Connection> connections = connectToParticipants(pid);
			if(connections.size() == 0){
				System.out.println("Process "+pid+" Failed: No participants are connected.");
				return false;
			}

			//map
			if(stopByPid.get(pid)){
				return false;
			}
			List<Partition<String>> input = Partition.fileToPartitions(configLoader.getInputFile().getPath(), configLoader.getPartitionSize());
			List<Partition<MRKeyVal>> mappedParts = startMapOrReduce("map", pid, connections, input, configLoader.getPartitionSize(), configLoader, null).getPartitions();
			if(mappedParts.isEmpty()){
				System.out.println("Process "+pid+" Failed: Map process failed, not individual participants.");
				return false;
			}
			mapDone.add(pid);

			//sort
			if(stopByPid.get(pid)){
				return false;
			}
			SortedMap<String,List<Partition<MRKeyVal>>> sortedParts = Sort.sort(mappedParts, configLoader.getPartitionSize());
			if(sortedParts.equals(null)){
				System.out.println("Process "+pid+" Failed: Sort process failed.");
				return false;
			}
			sortDone.add(pid);

			//reconnect to participants
			if(stopByPid.get(pid)){
				return false;
			}
			connections = connectToParticipants(pid);
			if(connections.size() == 0){
				System.out.println("Process "+pid+" Failed: No participants are connected.");
				return false;
			}

			//reduce
			if(stopByPid.get(pid)){
				return false;
			}
			boolean reduced = startMapOrReduce("reduce", pid, connections, null, configLoader.getPartitionSize(), configLoader, sortedParts).succeeded();
			if (!reduced){
				System.out.println("Process "+pid+" Failed: Reduce process failed, not individual participants.");
				return false;
			}

			return true;

		} catch (Exception e) {
			//All expected possible issues are handled above, this is a catch-all for any unexpected issues;
			//it just prints out the stack trace so you can debug because this would only be an odd issue
			e.printStackTrace();
			return false;
		}

	}

	public static void stopMapReduce(final int pid){
		stopByPid.put(pid, true);
		List<Thread> threads = new ArrayList<Thread>();
		int attempt = 0;
		boolean stopped = false;
		while(attempt < 5){
			if(writtenToFile.contains(pid)){
				System.out.println("Process "+pid+": Process completed before the command to stop was received.");
				return;
			}
			else if (reduceDone.contains(pid)){
				System.out.println("Process "+pid+": Process could not cleanly be stopped, it is finishing writing results to the output file.");
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
								connection.getOutputStream().flush();
								connection.getOutputStream().writeObject(stopCom);
								StopDone done = (StopDone) connection.getInputStream().readObject();
								if(!done.succeeded()){
									failures.add(connection);
								}
							} catch (Exception e) {
								//An error occurred with the connection to the participant, move on.
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
					//wait and try again
					Thread.sleep(1000);
				} catch (InterruptedException e) {
				}
				attempt++;
			}
		}
		if (stopped){
			System.out.println("Process "+pid+" has successfully been stopped.");
			connections = connectionsByPid.get(pid);
		}
		else{
			System.out.println("Process "+pid+": Process could not be cleanly stopped.");
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