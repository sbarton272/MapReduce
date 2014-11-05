package mapreduce;

import java.io.IOException;
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

		// Iterate through partitions
		for (KVPartition p : oldPartitions) {

			// Get partition values
			p.openRead();
			List<MRKeyVal> input = p.readAllContents();
			p.closeRead();

			// Iterate through partition values and map to new partition
			for (MRKeyVal val : input) {

				// TODO
				reduceFn.reduce(priorKV, val);
			}

			// Remove old partitions
			p.delete();

		}
		partitionWriter.close();

		// TODO test everything reduces to null

		return partitionWriter.getKVPartitions();


	}

}
