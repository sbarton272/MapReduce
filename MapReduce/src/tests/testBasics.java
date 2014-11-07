package tests;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import mapreduce.MRKeyVal;
import mapreduce.Map;
import mapreduce.Mapper;
import mapreduce.Reduce;
import mapreduce.Reducer;
import mergesort.MergeSort;
import fileIO.Partition;

public class testBasics {

	public static void main(String[] args) {

		int partitionSize = 5;
		Partition<MRKeyVal> p1 = new Partition<MRKeyVal>(partitionSize);

		try {
			p1.openWrite();
			p1.write(new MRKeyVal("foo", 2));
			p1.write(new MRKeyVal("bar", 2));
			p1.write(new MRKeyVal("zip", 2));
			p1.closeWrite();
		}catch (Exception e) {
			System.out.println("Ooops");
		}

		try {
			p1.openRead();
			System.out.println(p1.read().getKey());
			System.out.println(p1.read().getKey());
			System.out.println(p1.read().getKey());
			System.out.println(p1.read());
			p1.closeRead();
			System.out.println(p1.readAllContents());
			p1.delete();

		}catch (Exception e) {
			System.out.println("Ooops");
		}

		// Test sorting
		List<MRKeyVal> l = new ArrayList<MRKeyVal>();
		l.add(new MRKeyVal("c",2));
		l.add(new MRKeyVal("z",2));
		l.add(new MRKeyVal("a",2));
		List<MRKeyVal> l2 = new ArrayList<MRKeyVal>();
		l2.addAll(l);
		l2.add(new MRKeyVal("g",2));
		try {
			Partition<MRKeyVal> p2 = Partition.newFromKVList(l, 5);
			Partition<MRKeyVal> p3 = Partition.newFromKVList(l2, 5);
			List<Partition<MRKeyVal>> partitions = new ArrayList<Partition<MRKeyVal>>();
			partitions.add(p2);
			partitions.add(p3);

			List<Partition<MRKeyVal>> sorted = MergeSort.sort(partitions);

			// Print results
			Partition<MRKeyVal> p4 = sorted.get(0);
			System.out.println(p4.readAllContents());
			p4.delete();

			Partition<MRKeyVal> p5 = sorted.get(1);
			System.out.println(p5.readAllContents());
			p5.delete();
		} catch (IOException e) {
			System.out.println("Ooops");
		}

		try {

			// Load input file
			List<Partition<String>> input = Partition.fileToPartitions("resources/letters.txt", partitionSize);
			for(Partition<String> p : input) {
				System.out.print(p.readAllContents());
			}
			System.out.println();

			// Test map
			Mapper mapper = new Mapper(new MapLowerCase());
			List<Partition<MRKeyVal>> mapped = mapper.map(input, partitionSize);
			printAllPartitions(mapped);

			// Test sort
			List<Partition<MRKeyVal>> sorted = MergeSort.sort(mapped);
			printAllPartitions(sorted);

			// Test reduce
			Reducer reducer = new Reducer(new ReduceWordCount());
			List<Partition<MRKeyVal>> reduced = reducer.reduce(sorted, partitionSize);
			printAllPartitions(reduced);

			// Test write to output file
			String outFile = "resources/letterCount.txt";
			Partition.partitionsToFile(reduced, outFile, "-");
			List<Partition<String>> output = Partition.fileToPartitions(outFile, partitionSize);
			for(Partition<String> p : output) {
				System.out.print(p.readAllContents());
			}
			System.out.println();


		} catch (IOException e) {
			System.out.println("Ooops");
		}

		// TODO test null values and other edge cases

	}

	private static void printAllPartitions(List<Partition<MRKeyVal>> partitions) throws IOException {
		for(Partition<MRKeyVal> p : partitions) {
			System.out.print(p.readAllContents());
		}
		System.out.println();
	}
}

class MapLowerCase implements Map {

	@Override
	public MRKeyVal map(String input) {
		return new MRKeyVal(input.toLowerCase(), 1);
	}

}

class ReduceWordCount implements Reduce {

	@Override
	public MRKeyVal reduce(String key, List<Integer> values) {
		if (key == null) {
			return null;
		}

		int sum = 0;
		for (int val : values) {
			sum += val;
		}
		return new MRKeyVal(key, sum);
	}

}