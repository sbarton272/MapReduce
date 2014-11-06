package mapreduce;

import java.io.IOException;
import java.util.List;

import fileIO.Partition;
import fileIO.PartitionWriter;

/**
 * Handles mapping on the participant
 */
public class Mapper {

	private final Map mapFn;

	public Mapper(Map mapFn) {
		this.mapFn = mapFn;
	}

	public List<Partition<MRKeyVal>> map(List<Partition<String>> oldPartitions, int partitionSize) throws IOException {

		// TODO tmp dir per participant to get around AFS

		// Start partitionWriter to write mapped values
		PartitionWriter<MRKeyVal> partitionWriter = new PartitionWriter<MRKeyVal>(partitionSize);
		partitionWriter.open();

		// Iterate through partitions
		for (Partition<String> p : oldPartitions) {

			// Get partition values
			List<String> input = p.readAllContents();

			// Iterate through partition values and map to new partition
			for (String val : input) {

				// Map val with user defined map function
				MRKeyVal mappedVal = mapFn.map(val);

				// Do not write null values to new partition
				if (mappedVal != null) {
					partitionWriter.write(mappedVal);
				}
			}

			// Remove old partitions
			p.delete();

		}
		partitionWriter.close();

		// TODO test everything maps to null

		return partitionWriter.getPartitions();
	}

}
