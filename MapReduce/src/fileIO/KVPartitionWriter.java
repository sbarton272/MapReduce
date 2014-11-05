package fileIO;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import mapreduce.MRKeyVal;

/**
 * The KVPartitionWriter abstracts the process of writing to a
 * partition until full and then starting a new partition.
 * 
 * @author Spencer
 *
 */
public class KVPartitionWriter {

	private final int partitionSize;
	private boolean writeMode = false;
	private ArrayList<KVPartition> partitions;
	private KVPartition curKVPartition;

	public KVPartitionWriter(int partitionSize) {
		this.partitionSize = partitionSize;
	}

	public void open() throws IOException {
		writeMode = true;

		partitions = new ArrayList<KVPartition>();

		// Begin with a new partition to write
		curKVPartition = new KVPartition(partitionSize);
		curKVPartition.openWrite();

	}

	public List<KVPartition> close() throws IOException {

		// Need to be in writeMode to ensure that curKVPartition can be closed
		if (!writeMode){
			throw(new IOException("Need to open writer first"));
		}

		// Current partition has not been closed yet because it is not full
		curKVPartition.closeWrite();

		// Add curKVPartition to partitions if it has elements
		if (!curKVPartition.isEmpty()) {
			partitions.add(curKVPartition);
		}

		return partitions;
	}

	public void writeKeyVal(MRKeyVal val) throws IOException {

		// Need to be in writeMode to ensure that curKVPartition exists
		if (!writeMode){
			throw(new IOException("Need to open writer first"));
		}

		// If curKVPartition full, store and create a new partition
		if (curKVPartition.isFull()) {

			// Close current partition for writing
			curKVPartition.closeWrite();

			// Save references to full partitions
			partitions.add(curKVPartition);

			// Create new partition and open for writing
			curKVPartition = new KVPartition(partitionSize);
			curKVPartition.openWrite();
		}

		// Write value to curKVPartition
		curKVPartition.write(val);
	}

	public List<KVPartition> getKVPartitions() {
		return partitions;
	}

}
