package fileIO;

import java.io.IOException;
import java.util.List;
import java.util.TreeMap;

import mapreduce.MRKeyVal;

/**
 * Similar to PartitionWriter except that it starts a new partition when the key changes and creates KeyPartitions
 *
 */
public class KeyPartitionWriter {

	private final int partitionSize;
	private boolean firstElem = true;
	private final TreeMap<String,List<Partition<MRKeyVal>>> partitions;
	private PartitionWriter<MRKeyVal> curPartitionWriter;
	private String curKey;

	public KeyPartitionWriter(int partitionSize) throws IOException {
		this.partitionSize = partitionSize;
		partitions = new TreeMap<String,List<Partition<MRKeyVal>>>();

		// Keep track of first element so know when have a valid curKey
		firstElem = true;

		// Begin first partition writer
		curPartitionWriter = new PartitionWriter<MRKeyVal>(partitionSize);

	}

	public TreeMap<String, List<Partition<MRKeyVal>>> close() throws IOException {

		// Something was written so save remaining partitions
		if (!firstElem) {
			partitions.put(curKey, curPartitionWriter.close());
		}

		return partitions;
	}

	public void write(MRKeyVal kv) throws IOException {

		// Set cur key if this is the first element
		if (firstElem) {
			curKey = kv.getKey();
			firstElem = false;
		}

		// If new key discovered save the current partitions and start writing new partitions
		if (!curKey.equals(kv.getKey())) {

			// Close current partitions and save to partitions with their key
			partitions.put(curKey, curPartitionWriter.close());

			// Set new key and new partition writer
			curPartitionWriter = new PartitionWriter<MRKeyVal>(partitionSize);
			curKey = kv.getKey();
		}
		curPartitionWriter.write(kv);

	}

	public TreeMap<String, List<Partition<MRKeyVal>>> getPartitions() {
		return partitions;
	}
}
