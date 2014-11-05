package fileIO;

import java.io.IOException;
import java.util.List;

import mapreduce.MRKeyVal;

public class KVPartition extends Partition<MRKeyVal> {

	private static final long serialVersionUID = 8242693151634911206L;

	public KVPartition(int maxSize) {
		super(maxSize);
	}

	public static KVPartition newFromList(List<MRKeyVal> values, int maxSize) throws IOException {
		if(values.size() > maxSize) {
			throw(new IOException("Values too large"));
		}
		KVPartition result = new KVPartition(maxSize);

		// Shortcut the write operation by simply setting values as contents
		result.openWrite();
		result.contents = values;
		result.size = values.size();
		result.closeWrite();

		return result;
	}

}
