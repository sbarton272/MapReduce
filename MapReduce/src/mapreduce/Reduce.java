package mapreduce;

public interface Reduce {

	public MRKeyVal[] reduce(MRKeyVal[] priorVK, MRKeyVal curKV);

}
