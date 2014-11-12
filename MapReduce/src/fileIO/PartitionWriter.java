package fileIO;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * The PartitionWriter abstracts the process of writing to a
 * partition until full and then starting a new partition.
 * 
 * @author Spencer
 *
 */
public class PartitionWriter<T> {

	protected final int partitionSize;
	protected boolean writeMode = false;
	protected ArrayList<Partition<T>> partitions;
	protected Partition<T> curPartition;

	public PartitionWriter(int partitionSize) {
		this.partitionSize = partitionSize;
	}

	public void open() throws IOException {
		writeMode = true;

		partitions = new ArrayList<Partition<T>>();

		// Begin with a new partition to write
		curPartition = new Partition<T>(partitionSize);
		curPartition.openWrite();

	}

	public List<Partition<T>> close() throws IOException {

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

	public void write(T val) throws IOException {

		// Need to be in writeMode to ensure that curPartition exists
		if (!writeMode){
			throw(new IOException("Need to open writer first"));
		}

		// If curPartition full, store and create a new partition
		if (curPartition.isFull()) {

			// Close current partition for writing, nothing written if empty
			curPartition.closeWrite();

			// Save references to full partitions
			partitions.add(curPartition);

			// Create new partition and open for writing
			curPartition = new Partition<T>(partitionSize);
			curPartition.openWrite();
		}

		// Write value to curPartition
		curPartition.write(val);
	}

	public List<Partition<T>> getPartitions() {
		return partitions;
	}

}
