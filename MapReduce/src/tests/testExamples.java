package tests;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.SortedMap;

import mapreduce.MRKeyVal;
import mapreduce.Mapper;
import mapreduce.Reducer;
import master.ConfigLoader;
import sort.Sort;
import fileIO.Partition;

public class testExamples {

	public static void main(String[] args) {

		// Run Word Count
		ConfigLoader config;
		try {
			config = new ConfigLoader("examples/wordcount/wordcount.config");
			System.out.println(config.getJobname());

			// Perform operation
			List<Partition<String>> input = Partition.fileToPartitions(config.getInputFile().getPath(), config.getPartitionSize());
			Mapper mapper = new Mapper(config.getMapFn());
			List<Partition<MRKeyVal>> mapped = mapper.map(input, config.getPartitionSize());
			SortedMap<String, List<Partition<MRKeyVal>>> sorted = Sort.sort(mapped, config.getPartitionSize());
			Reducer reducer = new Reducer(config.getReduceFn());
			List<Partition<MRKeyVal>> reduced = reducer.reduce(sorted, config.getPartitionSize());
			Partition.partitionsToFile(reduced, config.getOutputFile().getPath(), "-");

			Partition.deleteAll(reduced);

		} catch (Exception e) {
			System.out.println("Unable to load config");
		}

		// Run Word Occurrences
		try {
			config = new ConfigLoader("examples/wordoccurences/wordoccurences.config");
			System.out.println(config.getJobname());

			// Perform operation
			List<Partition<String>> input = Partition.fileToPartitions(config.getInputFile().getPath(), config.getPartitionSize());
			Mapper mapper = new Mapper(config.getMapFn());
			List<Partition<MRKeyVal>> mapped = mapper.map(input, config.getPartitionSize());
			printAllPartitions(mapped);

			SortedMap<String,List<Partition<MRKeyVal>>> sorted = Sort.sort(mapped, config.getPartitionSize());
			List<Partition<MRKeyVal>> sortedList = flatten(sorted.values());
			printAllPartitions(sortedList);

			Reducer reducer = new Reducer(config.getReduceFn());
			List<Partition<MRKeyVal>> reduced = reducer.reduce(sorted, config.getPartitionSize());
			printAllPartitions(reduced);

			Partition.partitionsToFile(reduced, config.getOutputFile().getPath(), "-");

			Partition.deleteAll(reduced);

		} catch (Exception e) {
			System.out.println("Unable to load config");
		}

		System.out.println("Done");

	}

	private static void printAllPartitions(List<Partition<MRKeyVal>> partitions) throws IOException {
		System.out.print("Partitions("+partitions.size()+") ");
		for(Partition<MRKeyVal> p : partitions) {
			System.out.print(p.readAllContents());
		}
		System.out.println();
	}

	private static List<Partition<MRKeyVal>> flatten(Collection<List<Partition<MRKeyVal>>> collection) {
		List<Partition<MRKeyVal>> list = new ArrayList<Partition<MRKeyVal>>();
		for (List<Partition<MRKeyVal>> l : collection) {
			list.addAll(l);
		}
		return list;
	}
}
