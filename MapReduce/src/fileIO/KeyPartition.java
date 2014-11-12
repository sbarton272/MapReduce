package fileIO;

import java.io.IOException;

import mapreduce.MRKeyVal;

/**
 * Partition class of MRKeyVal that all have the same key
 */
public class KeyPartition extends Partition<MRKeyVal> {

	private static final long serialVersionUID = 6125528373918169361L;
	private final String key;

	public KeyPartition(int maxSize, String key) throws IOException {
		super(maxSize);
		this.key = key;
	}

	public KeyPartition(int maxSize, String hostName, String key) throws IOException {
		super(maxSize, hostName);
		this.key = key;
	}

	public String getKey() {
		return key;
	}

}
