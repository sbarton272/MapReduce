package mergesort;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import mapreduce.MRKeyVal;
import fileIO.Partition;

/**
 * Take in partitions and return new sorted partitions. Note that this removes the old partitions.
 * 
 * TODO Test this thoroughly
 */
public class Mergesort {

	public static Partition[] sort(Partition[] unsortedPartitions) {

		// Sort individual partitions
		// TODO can have mappers complete this step
		List<Partition> newPartitions = new ArrayList<Partition>();
		for (Partition partition : unsortedPartitions) {

			newPartitions.add(sortPartition(partition));

			// Remove old partition
			partition.delete();
		}

		// Merge partitions into new partitions
		List<Partition> sortedPartitions = mergePartitions(newPartitions);

		// Remove all old individually sorted partitions
		for (Partition partition : newPartitions) {
			partition.delete();
		}

		return (Partition[]) sortedPartitions.toArray();
	}

	//---------------------------------------------------------

	private static Partition sortPartition(Partition partition) {

		// Read whole partition into list of lists
		// This is the first split
		List<MRKeyVal> values = new ArrayList<MRKeyVal>();

		try {

			// Read in partition values
			partition.openRead();
			values = partition.getContents();
			partition.closeRead();

		} catch (IOException e) {
			e.printStackTrace();
		}

		Collections.sort(values); // TODO ordering?
		return Partition.newFromList(values, partition.getMaxSize());
	}

	/**
	 * Merge all partitions together at once
	 * Merge into a new partition until full and then create the next partition
	 */
	private static List<Partition> mergePartitions(List<Partition> partitions) {
		// TODO clean-up file IO try,catches - takes away from logic

		List<Partition> sortedPartitions = new ArrayList<Partition>();

		// Make new partitions the optimal size so know how much to fill each
		int newPartitionSize = partitions.get(0).getMaxSize();
		Partition curPartition = new Partition(newPartitionSize);

		// Continue merging until all partitions are merged
		MRKeyVal[] firstElems = new MRKeyVal[partitions.size()];

		// Populate first element array
		for(int i = 0; i < partitions.size(); i++) {
			try {

				// Open for reading, close when all done
				partitions.get(i).openRead();
				firstElems[i] = partitions.get(i).readKeyVal();

			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		// Open current partition
		try {
			curPartition.openWrite();
		} catch (IOException e) {
			e.printStackTrace();
		}

		// Iterate through all partitions and fill new partitions until all old partitions are empty
		int maxIndex;
		while(true) {
			// Get max of first elements
			maxIndex = findMaxIndex(firstElems);

			// If all first elems are null, no max so we are done
			if (maxIndex == -1) {
				break;
			}

			// Retrieved max elem from partition
			// Write value to curPartition
			try {
				curPartition.writeKeyVal(firstElems[maxIndex]);
			} catch (IOException e) {
				e.printStackTrace();
			}

			// If curPartition full, store and create a new partition
			if (curPartition.isFull()) {
				sortedPartitions.add(curPartition);

				// Close current partition for writing
				try {
					curPartition.closeWrite();
				} catch (IOException e) {
					e.printStackTrace();
				}

				// Create new parition and open for writing
				curPartition = new Partition(newPartitionSize);
				try {
					curPartition.openWrite();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}

			// Update the firstElems list
			try {
				firstElems[maxIndex] = partitions.get(maxIndex).readKeyVal();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		// Close old partitions because done reading
		for(int i = 0; i < partitions.size(); i++) {
			partitions.get(i).closeRead();
		}

		return sortedPartitions;
	}

	private static int findMaxIndex(MRKeyVal[] elems) {

		int maxIndex = -1;
		MRKeyVal max = null;
		for(int i = 0; i < elems.length; i++ ) {
			// TODO how does compare work?
			if ((elems[i] != null) && (elems[i].compareTo(max) >= 0)) {
				maxIndex = i;
				max = elems[i];
			}
		}
		return maxIndex;
	}

}
