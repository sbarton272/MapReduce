package fileIO;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;
import java.io.Serializable;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import mapreduce.MRKeyVal;
import messages.FileRequest;

public class Partition<T> implements Serializable {

	// TODO delete state?
	// TODO may be able to implement without serializable object stuff
	// TODO too much code here

	private static final long serialVersionUID = 2184080295517094612L;
	private static final String TMP_DIR = "tmp";
	private static final String EXT = ".partition";
	private final int maxSize;
	private List<T> contents;
	private final String filePath;
	private String hostName;
	private int size = 0;
	private long byteSize = 0;
	private boolean writeMode = false;
	private boolean readMode = false;
	private int curIndx;

	public Partition(int maxSize) {
		try {
			this.hostName = InetAddress.getLocalHost().getHostName();
		} catch (UnknownHostException e) {
			System.out.println("Parition unable to determine hostname");
			e.printStackTrace();
		}
		this.maxSize = maxSize;
		this.filePath = TMP_DIR + File.separator + Integer.toString(this.hashCode()) + EXT;;

		// TODO Make tmp directory if not present
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

		// Get number of bytes
		byteSize = new File(filePath).length();

		// Check that the file size is not too large to send over network
		if (byteSize > Integer.MAX_VALUE) {
			// TODO best idea? check elsewhere
			throw(new IOException("File too large"));
		}

		writeMode = false;

	}

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

	private void loadRemoteFile() throws IOException {

		Socket soc = null;
		ObjectOutputStream outStream = null;
		BufferedOutputStream fileOutStream = null;
		try {

			// Open socket to host machine
			soc = new Socket(hostName, FileServer.PORT);

			// Generate partition request message
			FileRequest req = new FileRequest(filePath, byteSize);

			// TODO times out if bad request?

			// Send message
			outStream = new ObjectOutputStream(
					soc.getOutputStream());
			outStream.writeObject(req);

			// Get input and file streams, create file at same filepath
			InputStream inStream = soc.getInputStream();
			fileOutStream = new BufferedOutputStream(new FileOutputStream(filePath));

			// Read stream and write to file
			byte[] bytes = new byte[(int) byteSize];
			int n;
			while( (n = inStream.read(bytes, 0, bytes.length)) > -1) {
				//System.out.println(n);
				fileOutStream.write(bytes, 0, n);
			}
			fileOutStream.flush();

			// Update host machine to this machine as this partition changes the local copy
			hostName = InetAddress.getLocalHost().getHostName();

		} catch (IOException e) {

			e.printStackTrace();

		} finally {
			// Close streams and socket
			if (outStream != null) {
				outStream.close();
			}
			if (fileOutStream != null) {
				fileOutStream.close();
			}
			if (soc != null) {
				soc.close();
			}
		}
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

	public String getFilePath() {
		return filePath;
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

	public void setHostName(String hostName) {
		// TODO remove this
		this.hostName = hostName;
	}

}
