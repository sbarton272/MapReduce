package mapreduce;

public interface Reduce {

	public MRKeyVal[] reduce(MRKeyVal[] priorKV, MRKeyVal curKV);

}
