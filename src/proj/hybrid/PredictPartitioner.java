package proj.hybrid;

import org.apache.hadoop.mapreduce.Partitioner;

public class PredictPartitioner extends Partitioner<Pair, Integer>{

	@Override
	public int getPartition(Pair key, Integer arg1, int reducer) {
		return key.first.hashCode() % reducer;
	}

}
