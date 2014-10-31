package fileIO;

import java.io.Serializable;

public class Partition implements Serializable {

	// TODO filelocation
	// TODO implements input stream?
	// TODO load file over

	public Partition() {
		// TODO create empty
		// generate name
	}

	public void open() {
		// Load if not present
	}

	public void close() {

	}

	public void readline() {

	}

	public void writeline() {
		// TODO buffered
	}

	public void delete() {

	}

	/**
	 * Given filepath read in file and split into N partitions
	 * 
	 */
	static Partition[] fileToPartitions(String filepath, int nPartitions) {
		return null;
	}

}
