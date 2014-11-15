package tests;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;

import mapreduce.MRKeyVal;
import mapreduce.MapFn;
import mapreduce.Mapper;
import mapreduce.ReduceFn;
import mapreduce.Reducer;
import sort.Sort;
import fileIO.Partition;

public class testBasics {

	public static void main(String[] args) {

		int partitionSize = 5;
		Partition<MRKeyVal> p1 = null;

		try {
			p1 = new Partition<MRKeyVal>(partitionSize);
			p1.openWrite();
			p1.write(new MRKeyVal("foo", 2));
			p1.write(new MRKeyVal("bar", 2));
			p1.write(new MRKeyVal("zip", 2));
			p1.closeWrite();
		}catch (Exception e) {
			System.out.println("Ooops");
			e.printStackTrace();
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
			e.printStackTrace();
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
			Partition<MRKeyVal> p2 = Partition.newFromKVList(l, partitionSize);
			Partition<MRKeyVal> p3 = Partition.newFromKVList(l2, partitionSize);
			List<Partition<MRKeyVal>> partitions = new ArrayList<Partition<MRKeyVal>>();
			partitions.add(p2);
			partitions.add(p3);

			Map<String, List<Partition<MRKeyVal>>> sorted = Sort.sort(partitions, partitionSize);

			// Print results
			Partition<MRKeyVal> p4 = sorted.get("a").get(0);
			System.out.println(p4.readAllContents());
			p4.delete();

			Partition<MRKeyVal> p5 = sorted.get("c").get(0);
			System.out.println(p5.readAllContents());
			p5.delete();

			Partition<MRKeyVal> p6 = sorted.get("g").get(0);
			System.out.println(p6.readAllContents());
			p6.delete();

			Partition<MRKeyVal> p7 = sorted.get("z").get(0);
			System.out.println(p7.readAllContents());
			p7.delete();

		} catch (IOException e) {
			System.out.println("Ooops");
			e.printStackTrace();
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
			SortedMap<String,List<Partition<MRKeyVal>>> sorted = Sort.sort(mapped, partitionSize);
			List<Partition<MRKeyVal>> sortedList = flatten(sorted.values());
			printAllPartitions(sortedList);

			// Test reduce
			Reducer reducer = new Reducer(new ReduceWordCount());
			List<Partition<MRKeyVal>> reduced = reducer.reduce(sorted, partitionSize);
			printAllPartitions(reduced);

			// Test write to output file
			String outFile = "resources/letterCount.txt";
			Partition.partitionsToFile(reduced, outFile, "-");
			List<Partition<String>> output = Partition.fileToPartitions(outFile, partitionSize);

			// Print all partitions and delete them
			for(Partition<String> p : output) {
				System.out.print(p.readAllContents());
				p.delete();
			}
			System.out.println();

			// Remove reduced
			for(Partition<MRKeyVal> p : reduced) {
				p.delete();
			}

		} catch (IOException e) {
			System.out.println("Ooops");
			e.printStackTrace();
		}

		// TODO test null values and other edge cases

	}

	private static List<Partition<MRKeyVal>> flatten(
			Collection<List<Partition<MRKeyVal>>> values) {
		List<Partition<MRKeyVal>> lst = new ArrayList<Partition<MRKeyVal>>();
		for(List<Partition<MRKeyVal>> l : values) {
			lst.addAll(l);
		}
		return lst;
	}

	private static void printAllPartitions(List<Partition<MRKeyVal>> partitions) throws IOException {
		for(Partition<MRKeyVal> p : partitions) {
			System.out.print(p.readAllContents());
		}
		System.out.println();
	}
}

class MapLowerCase implements MapFn {

	private static final long serialVersionUID = 3901767586735410035L;

	@Override
	public MRKeyVal map(String input) {
		return new MRKeyVal(input.toLowerCase(), 1);
	}

}

class ReduceWordCount implements ReduceFn {

	private static final long serialVersionUID = -377503573774051833L;

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