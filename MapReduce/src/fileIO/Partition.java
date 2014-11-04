package fileIO;

import java.io.File;
import java.io.Serializable;
import java.util.List;

import mapreduce.MRKeyVal;

public class Partition implements Serializable {

	// TODO filelocation
	// TODO implements input stream?
	// TODO load file over

	private static final long serialVersionUID = 2184080295517094612L;
	private final String TMP_DIR = "tmp";
	private final String filePath;
	private int optimalNumElems;
	private int numElems;

	/**
	 *  Create empty partition
	 * @param optimalPartitionSize
	 */
	public Partition(int optimalPartitionSize) {
		filePath = File.separator + TMP_DIR + File.separator + Integer.toString(this.hashCode());

		// Make tmp directory if not present
		// TODO


	}

	public void open() {
		// Load if not present


	}

	public void close() {

	}

	public void readline() {

	}

	public MRKeyVal readKeyVal() {
		return null; // TODO return null if at end
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

	public static Partition newFromList(List<MRKeyVal> values) {
		// TODO Auto-generated method stub
		return null;
	}

	public void writeKeyVal(MRKeyVal mrKeyVal) {
		// TODO Auto-generated method stub

	}

	public boolean isFull() {
		// TODO Auto-generated method stub
		return false;
	}

	public int getOptimalSize() {
		// TODO Auto-generated method stub
		return 0;
	}

}
