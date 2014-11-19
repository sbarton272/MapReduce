package master;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import mapreduce.MapFn;
import mapreduce.ReduceFn;

public class ConfigLoader {

	// Parse strings
	private final String STR_JOB_NAME = "JOB_NAME";
	private final String STR_INPUT_FILE = "INPUT_FILE";
	private final String STR_OUTPUT_FILE = "OUTPUT_FILE";
	private final String STR_MAP_FN = "MAP_FN";
	private final String STR_MAP_TIMEOUT_SEC = "MAP_TIMEOUT_SEC";
	private final String STR_REDUCE_FN = "REDUCE_FN";
	private final String STR_REDUCE_TIMEOUT_SEC = "REDUCE_TIMEOUT_SEC";
	private final String STR_NUM_REDUCERS = "NUM_REDUCERS";
	private final String STR_PARITION_SIZE = "PARITION_SIZE";
	private final String STR_MASTER = "MASTER";
	private final String STR_PARTICIPANT = "PARTICIPANT";
	private final String STR_FILE_SERVER_DIR = "FILE_SERVER_DIR";
	private final String STR_FILE_SERVER_PORT = "FILE_SERVER_PORT";
	private final String STR_KEY_DELIM = "=";
	private final String STR_DELIM = ":";

	// Config variables
	private String jobname = "";
	private File inputFile = null;
	private File outputFile = null;
	private MapFn mapFn = null;
	private int mapTimeoutSec = 10;
	private ReduceFn reduceFn = null;
	private int reduceTimeoutSec = 10;
	private int numReducers = 1;
	private int partitionSize = 64;
	private String masterHostName = "localhost";
	private int masterPort = 9042;
	private final List<ParticipantDetails> participants = new ArrayList<ParticipantDetails>();
	private final HashMap<String,String> userConfig = new HashMap<String,String>();
	private ParticipantDetails lastParticipantRecorded;
	private String fileServerDir;
	private int fileServerPort;

	public ConfigLoader(String filePath) throws IOException {
		File configFile = new File(filePath);
		BufferedReader reader = new BufferedReader(new FileReader(configFile));
		String line;
		while ((line = reader.readLine()) != null) {
			String[] split = line.split(STR_KEY_DELIM);
			if (split.length != 2) {
				reader.close();
				throw(new IOException("Invalid config file, follow KEY=VAL format"));
			}
			setKeyVal(split[0], split[1]);
		}
		reader.close();
	}

	private void setKeyVal(String key, String val) throws IOException {
		String[] hostPort;

		switch(key) {
		case STR_JOB_NAME:
			jobname = val;
			break;
		case STR_INPUT_FILE:
			inputFile = new File(val);

			// Check that input file exists
			if (!inputFile.exists()) {
				throw(new IOException("Loading config: input file does not exist"));
			}
			break;
		case STR_OUTPUT_FILE:
			outputFile = new File(val);
			break;
		case STR_MAP_FN:
			try {
				mapFn = (MapFn) loadClass(val);
			} catch (Exception e) {
				throw(new IOException("Unable to load mapFn"));
			}
			break;
		case STR_MAP_TIMEOUT_SEC:
			mapTimeoutSec = Integer.parseInt(val);
			break;
		case STR_REDUCE_FN:
			try {
				reduceFn = (ReduceFn) loadClass(val);
			} catch (Exception e) {
				throw(new IOException("Unable to load reduceFn"));
			}
			break;
		case STR_REDUCE_TIMEOUT_SEC:
			reduceTimeoutSec = Integer.parseInt(val);
			break;
		case STR_NUM_REDUCERS:
			numReducers = Integer.parseInt(val);
			break;
		case STR_PARITION_SIZE:
			partitionSize = Integer.parseInt(val);
			break;
		case STR_MASTER:
			hostPort = splitHostPort(val);
			masterHostName = hostPort[0];
			masterPort = Integer.parseInt(hostPort[1]);
			break;
		case STR_PARTICIPANT:
			hostPort = splitHostPort(val);
			lastParticipantRecorded = new ParticipantDetails(hostPort[0], hostPort[1]);
			participants.add(lastParticipantRecorded);
			break;
		case STR_FILE_SERVER_DIR:
			fileServerDir = val;
			break;
		case STR_FILE_SERVER_PORT:
			fileServerPort = Integer.parseInt(val);
			break;
		default:
			// Store all unrecognized keys to a list of KV pairs
			userConfig.put(key, key);
			break;
		}

	}

	//------------------------------------

	private String[] splitHostPort(String val) {
		String[] split = val.split(STR_DELIM);
		if (split.length != 2) {
			return null;
		}
		return split;
	}

	private Object loadClass(String val) throws Exception {
		Class<?> c = Class.forName(val);
		Constructor<?> ctor = c.getConstructor();
		return ctor.newInstance();
	}

	//------------------------------------

	public List<ParticipantDetails> getParticipants() {
		return participants;
	}

	public HashMap<String,String> getUserConfig() {
		return userConfig;
	}

	public String getJobname() {
		return jobname;
	}

	public File getInputFile() {
		return inputFile;
	}

	public File getOutputFile() {
		return outputFile;
	}

	public MapFn getMapFn() {
		return mapFn;
	}

	public int getMapTimeoutSec() {
		return mapTimeoutSec;
	}

	public ReduceFn getReduceFn() {
		return reduceFn;
	}

	public int getReduceTimeoutSec() {
		return reduceTimeoutSec;
	}

	public int getNumReducers() {
		return numReducers;
	}

	public int getPartitionSize() {
		return partitionSize;
	}

	public String getMasterHostName() {
		return masterHostName;
	}

	public int getMasterPort() {
		return masterPort;
	}

	public String getFileServerDir() {
		return fileServerDir;
	}

	public int getFileServerPort() {
		return fileServerPort;
	}
}
