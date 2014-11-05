package fileIO;

import java.util.List;

public class InputPartition extends Partition<String> {

	private static final long serialVersionUID = 4175737556313558370L;

	public InputPartition(int maxSize) {
		super(maxSize);
	}

	//------------------------------------------

	/**
	 * Given file path read in file and split into N partitions
	 */
	public static List<InputPartition> fileToPartitions(String filepath, int nPartitions) {
		// TODO
		return null;
	}

}
