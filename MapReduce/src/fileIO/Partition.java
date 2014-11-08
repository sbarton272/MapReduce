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
import java.net.InetAddress;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import mapreduce.MRKeyVal;

public class Partition<T> extends RemoteFile {

	// TODO delete state?
	// TODO may be able to implement without serializable object stuff
	// TODO too much code here

	private static final long serialVersionUID = 2184080295517094612L;
	private static final String TMP_DIR = FileServer.DEFAULT_REMOTE_FILE_DIR;
	private static final String EXT = ".partition";
	private final int maxSize;
	private List<T> contents;
	private final String filePath;
	private int size = 0;
	private boolean writeMode = false;
	private boolean readMode = false;
	private int curIndx;

	public Partition(int maxSize) throws IOException {
		// Generate hostName, this may not work
		this(maxSize, InetAddress.getLocalHost().getHostName());
	}

	public Partition(int maxSize, String hostName) throws IOException {

		// Generate filename
		super(TMP_DIR + File.separator + UUID.randomUUID() + EXT, hostName, 0);
		this.maxSize = maxSize;
		this.filePath =  this.file.getPath();

		System.out.println("Created partition: " + filePath);

		// Make tmp directory if not present
		File tmpDir = new File(TMP_DIR);
		tmpDir.mkdir();
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

		// Update file length
		long len = new File(filePath).length();
		if (len > Integer.MAX_VALUE) {
			throw(new IOException("File too large to be remote"));
		}
		setFileByteSize((int) len);

	}

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

	//------------------------------------------

	public void openRead() throws IOException {

		if (writeMode) {
			throw(new IOException());
		}

		// Load contents
		ObjectInputStream inStream;
		try {
			inStream = new ObjectInputStream(
					new FileInputStream(filePath));
		} catch (FileNotFoundException e) {

			// Try loading from remote
			load();
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
		} finally {
			if(inStream != null) inStream.close();
		}

		// Set reading state
		readMode = true;
		curIndx = 0;
	}

	public void closeRead() {
		readMode = false;
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

	public boolean isEmpty() {
		return size == 0;
	}

	public int getMaxSize() {
		return maxSize;
	}

	public int getSize() {
		return size;
	}

	public String getFilePath() {
		return filePath;
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
		// TODO remove from partition

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
