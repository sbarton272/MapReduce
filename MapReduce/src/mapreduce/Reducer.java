package mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import fileIO.KVPartition;
import fileIO.KVPartitionWriter;


/**
 * Handles reducing on the participant
 */
public class Reducer {

	private final Reduce reduceFn;

	public Reducer(Reduce reduceFn, List<MRKeyVal> reduceStart) {
		this.reduceFn = reduceFn;
	}

	public List<KVPartition> reduce(List<KVPartition> oldPartitions, int partitionSize) throws IOException {

		// Start partitionWriter to write reduced values
		KVPartitionWriter partitionWriter = new KVPartitionWriter(partitionSize);
		partitionWriter.open();

		// Values used to collect common key values
		List<Integer> commonValues = new ArrayList<Integer>();
		String curKey = null;

		// Iterate through partitions
		for (KVPartition p : oldPartitions) {

			// Get partition values
			p.openRead();
			List<MRKeyVal> input = p.readAllContents();
			p.closeRead();

			// Iterate through partition values and save to new partition
			// Collect values by key and once we have seen all of one key
			// we will reduce and save the result
			for (MRKeyVal keyVal : input) {

				if (keyVal.getKey().equals(curKey)) {
					commonValues.add(keyVal.getVal());
				} else {

					// New key so reduce old key and save results
					MRKeyVal reduceResult = reduceFn.reduce(curKey, commonValues);
					partitionWriter.writeKeyVal(reduceResult);

					// Set new key and reset commonValues
					curKey = keyVal.getKey();
					commonValues.clear();

				}
			}

			// Remove old partitions
			p.delete();

		}
		partitionWriter.close();

		return partitionWriter.getKVPartitions();
	}
}
