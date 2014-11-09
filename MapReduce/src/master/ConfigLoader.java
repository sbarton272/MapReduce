package master;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.HashMap;
import java.util.List;

import mapreduce.Map;
import mapreduce.Reduce;

public class ConfigLoader {

	// Consts
	private static final int DEFAULT_PARTICIPANT_PORT = 5454;

	// Parse strings
	private final String STR_JOB_NAME = "JOB_NAME";
	private final String STR_INPUT_FILE = "INPUT_FILE";
	private final String STR_OUTPUT_FILE = "OUTPUT_FILE";
	private final String STR_MAP_FN = "MAP_FN";
	private final String STR_MAP_TIMEOUT_SEC = "MAP_TIMEOUT_SEC";
	private final String STR_REDUCE_FN = "REDUCE_FN";
	private final String STR_REDUCE_TIMEOUT_SEC = "REDUCE_TIMEOUT_SEC";
	private final String STR_PARITION_SIZE = "PARITION_SIZE";
	private final String STR_MASTER = "MASTER";
	private final String STR_MASTER_PORT = "MASTER_PORT";
	private final String STR_PARTICIPANT = "PARTICIPANT";
	private final String STR_PARTICIPANT_PORT = "PARTICIPANT_PORT";
	private final String STR_KEY_DELIM = "=";

	// Config variables
	private String jobname = "";
	private File inputFile = null;
	private File outputFile = null;
	private Map mapFn = null;
	private int mapTimeoutSec = 10;
	private Reduce reduceFn = null;
	private int reduceTimeoutSec = 10;
	private int partitionSize = 64;
	private String masterHostName = "localhost";
	private int masterPort = 9042;
	private List<ParticipantDetails> participants;
	private HashMap<String,String> userConfig;
	private ParticipantDetails lastParticipantRecorded;

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
				mapFn = (Map) loadJar(new File(val));
			} catch (Exception e) {
				System.out.println("Loading config: unable to load " + val);
			}
			break;
		case STR_MAP_TIMEOUT_SEC:
			mapTimeoutSec = Integer.parseInt(val);
			break;
		case STR_REDUCE_FN:
			try {
				reduceFn = (Reduce) loadJar(new File(val));
			} catch (Exception e) {
				System.out.println("Loading config: unable to load " + val);
			}
			break;
		case STR_REDUCE_TIMEOUT_SEC:
			reduceTimeoutSec = Integer.parseInt(val);
			break;
		case STR_PARITION_SIZE:
			partitionSize = Integer.parseInt(val);
			break;
		case STR_MASTER:
			masterHostName = val;
			break;
		case STR_MASTER_PORT:
			masterPort = Integer.parseInt(val);
			break;
		case STR_PARTICIPANT:
			lastParticipantRecorded = new ParticipantDetails(val, DEFAULT_PARTICIPANT_PORT);
			participants.add(lastParticipantRecorded);
			break;
		case STR_PARTICIPANT_PORT:
			if (lastParticipantRecorded != null) {
				lastParticipantRecorded.setPort(Integer.parseInt(val));
			}
			break;
		default:
			// Store all unrecognized keys to a list of KV pairs
			userConfig.put(key, key);
			break;
		}

	}

	//------------------------------------

	private Object loadJar(File jar) throws Exception {
		// Used http://cvamshi.wordpress.com/2011/01/12/loading-jars-and-java-classes-dynamically/ to learn about how to do this

		URL jarURL = new URL("jar","","file:" + jar.getAbsolutePath() + "!/");
		URLClassLoader sysLoader = (URLClassLoader)ClassLoader.getSystemClassLoader();
		Class<URLClassLoader> sysClass = URLClassLoader.class;
		Method sysMethod = sysClass.getDeclaredMethod("addURL",new Class[] {URL.class});
		sysMethod.setAccessible(true);
		sysMethod.invoke(sysLoader, new Object[]{jarURL});
		URLClassLoader classLoader = URLClassLoader.newInstance(new URL[] {jarURL});
		Class<?> jarClass = classLoader.loadClass("mapreduce");
		return jarClass.newInstance();
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

	public Map getMapFn() {
		return mapFn;
	}

	public int getMapTimeoutSec() {
		return mapTimeoutSec;
	}

	public Reduce getReduceFn() {
		return reduceFn;
	}

	public int getReduceTimeoutSec() {
		return reduceTimeoutSec;
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

}
