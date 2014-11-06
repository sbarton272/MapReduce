package fileIO;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import mapreduce.MRKeyVal;

public class Partition<T> implements Serializable {

	// TODO filelocation
	// TODO implements input stream?
	// TODO load file over
	// TODO delete state?
	// TODO may be able to implement without serializable object stuff

	private static final long serialVersionUID = 2184080295517094612L;
	private static final String TMP_DIR = "tmp";
	private static final String EXT = ".partition";
	private final String filePath;
	protected List<T> contents;
	private final int maxSize;
	protected int size = 0;
	private boolean writeMode = false;
	private boolean readMode = false;
	private int curIndx;

	/**
	 *  Create empty partition
	 * @param optimalPartitionSize
	 */
	public Partition(int maxSize) {
		filePath = TMP_DIR + File.separator + Integer.toString(this.hashCode()) + EXT;
		this.maxSize = maxSize;
		size = 0;

		// TODO Make tmp directory if not present
		// TODO set uri to local location

	}

	//------------------------------------------

	public void openWrite() throws IOException {

		if (readMode) {
			throw(new IOException());
		}

		writeMode = true;

		// Create new file contents
		contents = new ArrayList<T>();
		size = 0;

		// Create tmp directory if not present
		if (Files.notExists(Paths.get(TMP_DIR))) {
			File tmp = new File(TMP_DIR);
			tmp.mkdir();
		}

	}

	public void closeWrite() throws IOException {

		// Open file to write
		ObjectOutputStream outStream = new ObjectOutputStream(
				new FileOutputStream(filePath));

		// Write contents
		outStream.writeObject(contents);
		outStream.close();

		writeMode = false;

	}

	public void openRead() throws IOException {

		if (writeMode) {
			throw(new IOException());
		}

		// Load contents, if necessary get remote copy
		ObjectInputStream inStream;
		try {
			inStream = new ObjectInputStream(
					new FileInputStream(filePath));
		} catch (FileNotFoundException e){

			// Try getting remote copy
			loadRemoteFile();
			inStream = new ObjectInputStream(
					new FileInputStream(filePath));
		}

		// Read in the file contents, messy syntax because of warning due to runtime type check
		try {
			@SuppressWarnings("unchecked")
			List<T> contents = (List<T>) inStream.readObject();
			this.contents = contents;
		} catch (ClassNotFoundException e) {

			// Do not expose underlying object reading functionality to the user
			throw(new IOException());
		}

		inStream.close();

		// Set reading state
		readMode = true;
		curIndx = 0;
	}

	private void loadRemoteFile() {
		// TODO Load if not present
	}

	public void closeRead() {
		readMode = false;
	}

	//------------------------------------------

	public void write(T val) throws IOException {
		// Do not write null value
		if (val == null) {
			return;
		}

		if(writeMode && (size < maxSize)){
			contents.add(val);
			size++;
		} else {
			throw(new IOException("Partition not open or full"));
		}
	}

	public T read() throws IOException {
		T result = null;
		if(readMode) {

			// Read current object unless at end
			if(curIndx < size) {
				result = contents.get(curIndx);
				curIndx++;
			} else {
				result = null;
			}
		} else {
			throw(new IOException("Open file for reading"));
		}
		return result;
	}

	public List<T> readAllContents() throws IOException {

		// Open read to refresh contents
		openRead();
		List<T> result = contents;
		closeRead();
		return result;
	}
	//------------------------------------------

	public void delete() {
		try {
			Files.delete(Paths.get(filePath));
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	//------------------------------------------

	public boolean isFull() {
		return size == maxSize;
	}

	public int getMaxSize() {
		return maxSize;
	}

	public int getSize() {
		return size;
	}

	public boolean isEmpty() {
		return size == 0;
	}

	//------------------------------------------

	public static Partition<MRKeyVal> newFromKVList(List<MRKeyVal> values, int maxSize) throws IOException {
		if(values.size() > maxSize) {
			throw(new IOException("Values too large"));
		}
		Partition<MRKeyVal> result = new Partition<MRKeyVal>(maxSize);

		// Shortcut the write operation by simply setting values as contents
		result.openWrite();
		result.contents = values;
		result.size = values.size();
		result.closeWrite();

		return result;
	}

	/**
	 * Given file path read in file and split into N partitions
	 * @throws IOException
	 */
	public static List<Partition<String>> fileToPartitions(String filepath, int partitionSize) throws IOException {

		// Open file
		BufferedReader reader = new BufferedReader(new FileReader(filepath));

		// Partition writer to create partitions
		PartitionWriter<String> partitionWriter = new PartitionWriter<String>(partitionSize);
		partitionWriter.open();

		// Iterate through input file
		String line = reader.readLine();
		while (line != null) {
			partitionWriter.write(line);
			line = reader.readLine();
		}

		// Close reader, partitionWriter and file
		reader.close();
		partitionWriter.close();

		return partitionWriter.getPartitions();
	}

	/**
	 * Write partitions out to an output file
	 * @throws IOException
	 */
	public static void partitionsToFile(List<Partition<MRKeyVal>> partitions, String filepath, String delimeter) throws IOException {

		PrintWriter writer = new PrintWriter(filepath);

		for (Partition<MRKeyVal> p : partitions) {
			List<MRKeyVal> contents = p.readAllContents();
			for (MRKeyVal kv : contents) {
				writer.println(kv.getKey() + delimeter + Integer.toString(kv.getVal()));
			}
		}
		writer.close();
	}

}
