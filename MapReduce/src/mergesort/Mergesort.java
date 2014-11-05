package mergesort;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import mapreduce.MRKeyVal;
import fileIO.Partition;
import fileIO.PartitionWriter;

/**
 * Take in partitions and return new sorted partitions. Note that this removes the old partitions.
 * 
 * TODO Test this thoroughly
 */
public class Mergesort {

	public static List<Partition> sort(List<Partition> unsortedPartitions) {

		// Sort individual partitions
		// TODO can have mappers complete this step
		List<Partition> newPartitions = new ArrayList<Partition>();
		for (Partition partition : unsortedPartitions) {

			newPartitions.add(sortPartition(partition));

			// Remove old partition
			partition.delete();
		}

		// Merge partitions into new partitions
		List<Partition> sortedPartitions = mergePartitions(newPartitions, newPartitions.get(0).getMaxSize());

		// Remove all old individually sorted partitions
		for (Partition partition : newPartitions) {
			partition.delete();
		}

		return sortedPartitions;
	}

	//---------------------------------------------------------

	private static Partition sortPartition(Partition partition) {

		// Read whole partition into list of lists
		// This is the first split
		List<MRKeyVal> values = new ArrayList<MRKeyVal>();

		try {

			// Read in partition values
			partition.openRead();
			values = partition.readAllContents();
			partition.closeRead();

		} catch (IOException e) {
			e.printStackTrace();
		}

		Collections.sort(values); // TODO ordering?
		Partition result = null;
		try {
			result = Partition.newFromList(values, partition.getMaxSize());
		} catch (IOException e) {
			e.printStackTrace();
		}
		return result;
	}

	/**
	 * Merge all partitions together at once
	 * Merge into a new partition until full and then create the next partition
	 */
	private static List<Partition> mergePartitions(List<Partition> partitions, int newPartitionSize) {

		List<Partition> sortedPartitions = new ArrayList<Partition>();
		try {

			// Continue merging until all partitions are merged
			MRKeyVal[] firstElems = populateFirstElems(partitions);

			// Make new partitions the optimal size so know how much to fill each
			PartitionWriter partitionWriter = new PartitionWriter(newPartitionSize);

			// Open partition writer, this will write partitions until full and then fill a new one
			partitionWriter.open();

			// Iterate through all partitions and fill new partitions until all old partitions are empty
			int minIndex;
			while(true) {
				// Get max of first elements
				minIndex = findMinIndex(firstElems);

				// If all first elems are null, no max so we are done
				if (minIndex == -1) {
					break;
				}

				// Retrieved max elem from partition
				// Write value to partition
				partitionWriter.writeKeyVal(firstElems[minIndex]);

				// Update the firstElems list
				firstElems[minIndex] = partitions.get(minIndex).readKeyVal();
			}

			// Finally close last partition and get all created partitions
			sortedPartitions = partitionWriter.close();

			// Close old partitions because done reading
			for(int i = 0; i < partitions.size(); i++) {
				partitions.get(i).closeRead();
			}

		} catch (IOException e) {
			sortedPartitions = null;
		}
		return sortedPartitions;
	}

	private static MRKeyVal[] populateFirstElems(List<Partition> partitions) throws IOException {
		// Continue merging until all partitions are merged
		MRKeyVal[] firstElems = new MRKeyVal[partitions.size()];

		// Populate first element array
		for(int i = 0; i < partitions.size(); i++) {
			// Open for reading, close when all done
			partitions.get(i).openRead();
			firstElems[i] = partitions.get(i).readKeyVal();
		}
		return firstElems;
	}

	private static int findMinIndex(MRKeyVal[] elems) {

		int minIndex = -1;
		MRKeyVal min = null;
		for(int i = 0; i < elems.length; i++ ) {
			if (elems[i] != null) {
				if ((min == null) || (elems[i].compareTo(min) <= 0)) {
					minIndex = i;
					min = elems[i];
				}
			}
		}
		return minIndex;
	}

}
