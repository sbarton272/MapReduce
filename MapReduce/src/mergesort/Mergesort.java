package mergesort;

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
		List<Partition> newPartitions = new List<Partition>();
		for (Partition partition : unsortedPartitions) {
			newPartitions.add(sortPartition(partition));
		}

		// Merge partitions into new partitions
		List<Partition> sortedPartitions = mergePartitions(newPartitions);

		// Remove all old individually sorted partitions
		// TODO

		return (Partition[]) sortedPartitions.toArray();
	}

	//---------------------------------------------------------

	private static Partition sortPartition(Partition partition) {

		// Read whole partition into list of lists
		// This is the first split
		List<MRKeyVal> values = new List<MRKeyVal>();

		partition.open();
		MRKeyVal value = partition.readKeyVal();
		while(value != null) {
			values.add(value);
			value = partition.readKeyVal();
		}
		partition.close();

		// Remove old partition
		partition.delete();

		Collections.sort(values); // TODO ordering?
		return Partition.newFromList(values);
	}

	private static List<Partition> mergePartitions(List<Partition> partitions) {
		// Merge all partitions together at once
		// Merge into a new partition until full and then create the next partition

		List<Partition> sortedPartitions = new List<Partition>();
		Partition curPartiton = new Partition();

		// Continue merging until all partitions are merged
		MRKeyVal[] firstElems = new MRKeyVal[partitions.size()];
		// Populate first element array
		for(int i; i < partitions.size(); i++) {
			firstElems[i] = partitions.get(i).readKeyVal();
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

			//
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
