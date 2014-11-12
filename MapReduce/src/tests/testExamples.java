package tests;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import mapreduce.MRKeyVal;
import mapreduce.Mapper;
import mapreduce.Reducer;
import master.ConfigLoader;
import mergesort.MergeSort;
import fileIO.KeyPartition;
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
			List<Partition<MRKeyVal>> sorted = flatten(MergeSort.sort(mapped, config.getPartitionSize()));
			Reducer reducer = new Reducer(config.getReduceFn());
			List<Partition<MRKeyVal>> reduced = reducer.reduce(sorted, config.getPartitionSize());
			Partition.partitionsToFile(reduced, config.getOutputFile().getPath(), "-");

			Partition.deleteAll(reduced);

		} catch (IOException e) {
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
			List<Partition<MRKeyVal>> sorted = flatten(MergeSort.sort(mapped, config.getPartitionSize()));
			Reducer reducer = new Reducer(config.getReduceFn());
			List<Partition<MRKeyVal>> reduced = reducer.reduce(sorted, config.getPartitionSize());
			Partition.partitionsToFile(reduced, config.getOutputFile().getPath(), "-");

			List<Partition<String>> check = Partition.fileToPartitions(config.getOutputFile().getPath(), 5);
			System.out.println(check.get(0));

			Partition.deleteAll(check);
			Partition.deleteAll(reduced);

		} catch (IOException e) {
			System.out.println("Unable to load config");
		}


	}

	private static List<Partition<MRKeyVal>> flatten(List<List<KeyPartition>> list2) {
		List<Partition<MRKeyVal>> list = new ArrayList<Partition<MRKeyVal>>();
		for (List<KeyPartition> l : list2) {
			list.addAll(l);
		}
		return list;
	}
}
