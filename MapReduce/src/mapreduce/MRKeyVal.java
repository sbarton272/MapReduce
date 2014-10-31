package mapreduce;

import java.io.Serializable;

public class MRKeyVal implements Serializable {

	private static final long serialVersionUID = -5307943868247627381L;
	private final Comparable<Object> key;
	private final Object val;

	public MRKeyVal(Comparable<Object> key, Object val) {
		this.key = key;
		this.val = val;
	}

	public Comparable<Object> getkey() {
		return key;
	}

	public Object getVal() {
		return val;
	}

}
