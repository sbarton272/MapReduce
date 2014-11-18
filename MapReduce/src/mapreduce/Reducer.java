package mapreduce;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.SortedMap;

import fileIO.Partition;
import fileIO.PartitionWriter;


/**
 * Handles reducing on the participant
 */
public class Reducer implements Serializable{

	private final ReduceFn reduceFn;

	public Reducer(ReduceFn reduceFn) {
		this.reduceFn = reduceFn;
	}

	public List<Partition<MRKeyVal>> reduce(SortedMap<String,List<Partition<MRKeyVal>>> oldPartitions, int partitionSize) throws IOException {

		// Start partitionWriter to write reduced values
		PartitionWriter<MRKeyVal> partitionWriter = new PartitionWriter<MRKeyVal>(partitionSize);

		// Iterate through partitions, each partition will have only one key type
		// Gather all common key types before reducing
		for (Entry<String, List<Partition<MRKeyVal>>> partitions : oldPartitions.entrySet()) {

			// Collect all values before reducing
			List<Integer> commonValues = new ArrayList<Integer>();
			for(Partition<MRKeyVal> p : partitions.getValue()) {

				// Add all partition values but not keys (and double check for sanity that keys are same)
				for(MRKeyVal kv : p.readAllContents()) {

					// Test invariant
					if (!kv.getKey().equals(partitions.getKey())) {
						throw(new IOException("Partitions are not by key"));
					}

					commonValues.add(kv.getVal());
				}

				// Remove old partitions
				p.delete();

			}

			// Reduce for this key
			MRKeyVal reduceResult = reduceFn.reduce(partitions.getKey(), commonValues);
			if (reduceResult != null) {
				partitionWriter.write(reduceResult);
			}

		}
		partitionWriter.close();

		return partitionWriter.getPartitions();
	}
}
