package fileIO;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

public abstract class Partition<T> implements Serializable {

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
		if(readMode) {
			return contents;
		} else {
			throw(new IOException("Open file for reading"));
		}
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

}
