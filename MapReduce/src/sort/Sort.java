package sort;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;

import mapreduce.MRKeyVal;
import fileIO.KeyPartitionWriter;
import fileIO.Partition;

/**
 * Take in partitions and return new sorted partitions. Note that this removes the old partitions.
 * 
 * TODO Test this thoroughly
 */
public class Sort {

	public static SortedMap<String, List<Partition<MRKeyVal>>> sort(List<Partition<MRKeyVal>> unsortedPartitions, int partitionSize) {

		// Sort individual partitions
		List<Partition<MRKeyVal>> newPartitions = new ArrayList<Partition<MRKeyVal>>();
		for (Partition<MRKeyVal> partition : unsortedPartitions) {

			newPartitions.add(sortPartition(partition));

			// Remove old partition
			partition.delete();
		}

		// Merge partitions into new partitions
		SortedMap<String,List<Partition<MRKeyVal>>> sortedPartitions = mergePartitions(newPartitions, partitionSize);

		// Remove all old individually sorted partitions
		for (Partition<MRKeyVal> partition : newPartitions) {
			partition.delete();
		}

		return sortedPartitions;
	}

	//---------------------------------------------------------

	private static Partition<MRKeyVal> sortPartition(Partition<MRKeyVal> partition) {

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

		// Sort values
		Collections.sort(values);

		// Store to new partition
		Partition<MRKeyVal> result = null;
		try {
			result = Partition.newFromKVList(values, partition.getMaxSize());
		} catch (IOException e) {
			e.printStackTrace();
		}
		return result;
	}

	/**
	 * Merge all partitions together at once. Stores entries by key in a map.
	 * The values of the map are lists of same-key partitions.
	 */
	private static SortedMap<String,List<Partition<MRKeyVal>>> mergePartitions(List<Partition<MRKeyVal>> partitions, int newPartitionSize) {

		SortedMap<String, List<Partition<MRKeyVal>>> sortedPartitions = new TreeMap<String,List<Partition<MRKeyVal>>>();
		try {

			// Continue merging until all partitions are merged
			MRKeyVal[] firstElems = populateFirstElems(partitions);

			// Make new partitions the specified size by key
			KeyPartitionWriter partitionWriter = new KeyPartitionWriter(newPartitionSize);

			// Iterate through all partitions and fill new partitions until all old partitions are empty
			// Also switch partitions when a new key appears
			int minIndex;
			while(true) {
				// Get max of first elements
				minIndex = findMinIndex(firstElems);

				// If all first elems are null, no max so we are done
				if (minIndex == -1) {
					break;
				}

				// Retrieved max elem from partition
				partitionWriter.write(firstElems[minIndex]);

				// Update the firstElems list
				firstElems[minIndex] = partitions.get(minIndex).read();
			}

			// Finally close partition writer and get all created partitions
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

	private static MRKeyVal[] populateFirstElems(List<Partition<MRKeyVal>> partitions) throws IOException {
		// Continue merging until all partitions are merged
		MRKeyVal[] firstElems = new MRKeyVal[partitions.size()];

		// Populate first element array
		for(int i = 0; i < partitions.size(); i++) {
			// Open for reading, close when all done
			partitions.get(i).openRead();
			firstElems[i] = partitions.get(i).read();
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
