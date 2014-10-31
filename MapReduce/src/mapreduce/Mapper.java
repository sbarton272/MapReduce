package mapreduce;

import fileIO.Partition;

/**
 * Handles mapping on the participant
 */
public class Mapper {

	private final Map mapFn;

	public Mapper(Map mapFn) {
		this.mapFn = mapFn;
	}

	public Partition[] map(Partition[] oldPartitions) {
		return oldPartitions;
		// TODO remove old when done with new
		// TODO tmp dir per participant to get around AFS
	}

}
