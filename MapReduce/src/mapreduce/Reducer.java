package mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import fileIO.Partition;
import fileIO.PartitionWriter;


/**
 * Handles reducing on the participant
 */
public class Reducer {

	private final ReduceFn reduceFn;

	public Reducer(ReduceFn reduceFn) {
		this.reduceFn = reduceFn;
	}

	public List<Partition<MRKeyVal>> reduce(List<Partition<MRKeyVal>> oldPartitions, int partitionSize) throws IOException {
		// TODO support n reducers with n output files

		// Start partitionWriter to write reduced values
		PartitionWriter<MRKeyVal> partitionWriter = new PartitionWriter<MRKeyVal>(partitionSize);

		// Values used to collect common key values
		List<Integer> commonValues = new ArrayList<Integer>();
		String curKey = null;

		// Iterate through partitions
		for (Partition<MRKeyVal> p : oldPartitions) {

			// Get partition values
			List<MRKeyVal> input = p.readAllContents();

			// Iterate through partition values and save to new partition
			// Collect values by key and once we have seen all of one key
			// we will reduce and save the result
			for (MRKeyVal keyVal : input) {

				if (keyVal.getKey().equals(curKey)) {
					commonValues.add(keyVal.getVal());
				} else {

					// Do not give reduce null key or empty common values
					if ((curKey != null) && (!commonValues.isEmpty())) {

						// New key so reduce old key and save results
						MRKeyVal reduceResult = reduceFn.reduce(curKey, commonValues);
						partitionWriter.write(reduceResult);
					}

					// Set new key and reset commonValues
					curKey = keyVal.getKey();
					commonValues.clear();

					// Add current value
					commonValues.add(keyVal.getVal());

				}
			}

			// Remove old partitions
			p.delete();

		}
		partitionWriter.close();

		return partitionWriter.getPartitions();
	}
}
