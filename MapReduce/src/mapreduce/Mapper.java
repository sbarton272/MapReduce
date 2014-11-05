package mapreduce;

import java.io.IOException;
import java.util.List;

import fileIO.InputPartition;
import fileIO.KVPartition;
import fileIO.KVPartitionWriter;

/**
 * Handles mapping on the participant
 */
public class Mapper {

	private final Map mapFn;

	public Mapper(Map mapFn) {
		this.mapFn = mapFn;
	}

	public List<KVPartition> map(List<InputPartition> oldPartitions, int partitionSize) throws IOException {

		// TODO tmp dir per participant to get around AFS

		// Start partitionWriter to write mapped values
		KVPartitionWriter partitionWriter = new KVPartitionWriter(partitionSize);
		partitionWriter.open();

		// Iterate through partitions
		for (InputPartition p : oldPartitions) {

			// Get partition values
			p.openRead();
			List<String> input = p.readAllContents();
			p.closeRead();

			// Iterate through partition values and map to new partition
			for (String val : input) {

				// Map val with user defined map function
				MRKeyVal mappedVal = mapFn.map(val);

				// Do not write null values to new partition
				if (mappedVal != null) {
					partitionWriter.writeKeyVal(mappedVal);
				}
			}

			// Remove old partitions
			p.delete();

		}
		partitionWriter.close();

		// TODO test everything maps to null

		return partitionWriter.getKVPartitions();
	}

}
