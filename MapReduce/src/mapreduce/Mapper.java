package mapreduce;

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

	public List<Partition> map(List<Partition> oldPartitions, int partitionSize) {

		// Start partitionWriter to write mapped values
		PartitionWriter partitionWriter = new PartitionWriter(partitionSize);
		partitionWriter.open();

		// Iterate through partitions
		for (Partition p : oldPartitions) {

			// Get partition values
			p.openRead();
			List<MRKeyVal> input = p.readAllContents();
			p.closeRead();

			// Iterate through partition values and map to new partition
			for (MRKeyVal val : input) {

				// Map val with user defined map function
				// TODO

				// Do not write null values to new partition
				// TODO

				// TODO special input partition type to just have string
			}
		}

		partitionWriter.close();

		return partitionWriter.getPartitions();
		// TODO remove old when done with new
		// TODO tmp dir per participant to get around AFS
	}

}
