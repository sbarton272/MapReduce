package fileIO;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import mapreduce.MRKeyVal;

/**
 * Similar to PartitionWriter except that it starts a new partition when the key changes and creates KeyPartitions
 *
 */
public class KeyPartitionWriter {

	private final int partitionSize;
	private boolean writeMode = false;
	private boolean firstElem = true;
	private List<List<KeyPartition>> partitions;
	private List<KeyPartition> curKeyList;
	private KeyPartition curPartition;
	private String curKey;

	public KeyPartitionWriter(int partitionSize) {
		this.partitionSize = partitionSize;
	}

	public void open() throws IOException {
		writeMode = true;
		firstElem = true;

		partitions = new ArrayList<List<KeyPartition>>();

		// Begin with a new list of same key partitions
		curKeyList = new ArrayList<KeyPartition>();

	}

	public List<List<KeyPartition>> close() throws IOException {

		// Need to be in writeMode to ensure that curPartition can be closed
		if (!writeMode){
			throw(new IOException("Need to open writer first"));
		}

		// Something was written
		if (!firstElem) {

			// Current partition has not been closed yet because it is not full
			curPartition.closeWrite();

			// Add curPartition to partitions if it has elements
			if (!curPartition.isEmpty()) {
				curKeyList.add(curPartition);
				partitions.add(curKeyList);
			}
		}

		return partitions;
	}

	public void write(MRKeyVal kv) throws IOException {

		// Need to be in writeMode to ensure that curPartition exists
		if (!writeMode){
			throw(new IOException("Need to open writer first"));
		}

		// Set cur key if this is the first elem
		if (firstElem) {
			curKey = kv.getKey();

			// Begin with a new partition to write
			curPartition = new KeyPartition(partitionSize, curKey);
			curPartition.openWrite();

			firstElem = false;
		}

		// If curPartition full or new key, store and create a new partition
		if (curPartition.isFull()) {

			// Close current partition for writing
			curPartition.closeWrite();

			// Save to curKeyList
			curKeyList.add(curPartition);

			// Create new partition and open for writing
			curPartition = new KeyPartition(partitionSize, curKey);
			curPartition.openWrite();

		} else if (!curKey.equals(kv.getKey())) {

			// Save last key partition
			curPartition.closeWrite();
			curKeyList.add(curPartition);

			// Done with this key so add curKeyList to partitions
			partitions.add(curKeyList);

			// Create new partition and new curKeyList
			curPartition = new KeyPartition(partitionSize, curKey);
			curPartition.openWrite();
			curKeyList = new ArrayList<KeyPartition>();

			// Update current key
			curKey = kv.getKey();

		}

		// Write value to curPartition
		curPartition.write(kv);
	}

	public List<List<KeyPartition>> getPartitions() {
		return partitions;
	}
}
