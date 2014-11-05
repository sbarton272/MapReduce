package mapreduce;

import java.io.Serializable;

public class MRKeyVal implements Serializable, Comparable<MRKeyVal> {

	private static final long serialVersionUID = -5307943868247627381L;
	private final String key;
	private final int val;

	public MRKeyVal(String key, int val) {
		this.key = key;
		this.val = val;
	}

	public String getKey() {
		return key;
	}

	public int getVal() {
		return val;
	}

	@Override
	public int compareTo(MRKeyVal o) {
		return key.compareTo(o.getKey());
	}

	@Override
	public String toString() {
		return key + ":" + Integer.toString(val);
	}

}
