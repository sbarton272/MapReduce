package fileIO;

import java.io.IOException;
import java.util.ArrayList;

import mapreduce.MRKeyVal;

/**
 * Similar to PartitionWriter except that it starts a new partition when the key changes and creates KeyPartitions
 *
 */
public class KeyPartitionWriter extends PartitionWriter<MRKeyVal> {

	private String curKey = "";

	public KeyPartitionWriter(int partitionSize) {
		super(partitionSize);
	}

	@Override
	public void open() throws IOException {
		writeMode = true;

		partitions = new ArrayList<Partition<MRKeyVal>>();

		curKey = null;

	}

	@Override
	public void write(MRKeyVal kv) throws IOException {

		// Need to be in writeMode to ensure that curPartition exists
		if (!writeMode){
			throw(new IOException("Need to open writer first"));
		}

		// If curKey is null this is the first write so start curPartition
		if (curKey == null) {

			// Begin with a new partition to write
			curPartition = new KeyPartition(partitionSize, "");
			curPartition.openWrite();

			// Set curKey to first key
			curKey = kv.getKey();
		}

		// If curPartition full or new key, store and create a new partition
		if (curPartition.isFull() || (!curKey.equals(kv.getKey()))) {

			// Update key (if full will stay the same)
			curKey = kv.getKey();
			// TODO key is null

			// Close current partition for writing
			curPartition.closeWrite();

			// Save references to full partitions
			partitions.add(curPartition);

			// Create new partition and open for writing
			curPartition = new KeyPartition(partitionSize, kv.getKey());
			curPartition.openWrite();
		}

		// Write value to curPartition
		curPartition.write(kv);
	}

}
