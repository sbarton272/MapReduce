package fileIO;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import mapreduce.MRKeyVal;

/**
 * The PartitionWriter abstracts the process of writing to a
 * partition until full and then starting a new partition.
 * 
 * @author Spencer
 *
 */
public class PartitionWriter {

	private final int partitionSize;
	private boolean writeMode = false;
	private ArrayList<Partition> partitions;
	private Partition curPartition;

	public PartitionWriter(int partitionSize) {
		this.partitionSize = partitionSize;
	}

	public void open() throws IOException {
		writeMode = true;

		partitions = new ArrayList<Partition>();

		// Begin with a new partition to write
		curPartition = new Partition(partitionSize);
		curPartition.openWrite();

	}

	public List<Partition> close() throws IOException {

		// Need to be in writeMode to ensure that curPartition can be closed
		if (!writeMode){
			throw(new IOException("Need to open writer first"));
		}

		// Current partition has not been closed yet because it is not full
		curPartition.closeWrite();

		// Add curPartition to partitions if it has elements
		if (!curPartition.isEmpty()) {
			partitions.add(curPartition);
		}

		return partitions;
	}

	public void writeKeyVal(MRKeyVal val) throws IOException {

		// Need to be in writeMode to ensure that curPartition exists
		if (!writeMode){
			throw(new IOException("Need to open writer first"));
		}

		// If curPartition full, store and create a new partition
		if (curPartition.isFull()) {

			// Close current partition for writing
			curPartition.closeWrite();

			// Save references to full partitions
			partitions.add(curPartition);

			// Create new partition and open for writing
			curPartition = new Partition(partitionSize);
			curPartition.openWrite();
		}

		// Write value to curPartition
		curPartition.writeKeyVal(val);
	}

	public List<Partition> getPartitions() {
		return partitions;
	}

}
