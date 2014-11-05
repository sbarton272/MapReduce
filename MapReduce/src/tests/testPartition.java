package tests;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import mapreduce.MRKeyVal;
import mergesort.Mergesort;
import fileIO.Partition;

public class testPartition {

	public static void main(String[] args) {

		int partitionSize = 5;
		Partition p1 = new Partition(partitionSize);

		try {
			p1.openWrite();
			p1.writeKeyVal(new MRKeyVal("foo", 2));
			p1.writeKeyVal(new MRKeyVal("bar", 2));
			p1.writeKeyVal(new MRKeyVal("zip", 2));
			p1.closeWrite();
		}catch (Exception e) {
			System.out.println("Ooops");
		}

		try {
			p1.openRead();
			System.out.println(p1.readKeyVal().getKey());
			System.out.println(p1.readKeyVal().getKey());
			System.out.println(p1.readKeyVal().getKey());
			System.out.println(p1.readKeyVal());
			p1.closeRead();
			p1.openRead();
			System.out.println(p1.readAllContents());
			p1.closeRead();
			p1.delete();
			p1.openRead();

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
			Partition p2 = Partition.newFromList(l, 5);
			Partition p3 = Partition.newFromList(l2, 5);
			List<Partition> partitions = new ArrayList<Partition>();
			partitions.add(p2);
			partitions.add(p3);

			List<Partition> sorted = Mergesort.sort(partitions);

			// Print results
			Partition p4 = sorted.get(0);
			p4.openRead();
			System.out.println(p4.readAllContents());
			p4.closeRead();
			p4.delete();

			Partition p5 = sorted.get(1);
			p5.openRead();
			System.out.println(p5.readAllContents());
			p5.closeRead();
			p5.delete();
		} catch (IOException e) {
			System.out.println("Ooops");
		}


	}

}
