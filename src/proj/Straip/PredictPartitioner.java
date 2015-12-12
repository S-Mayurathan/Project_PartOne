package proj.Straip;

import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class PredictPartitioner extends Partitioner<Text, MapWritable>{

	@Override
	public int getPartition(Text key, MapWritable arg1, int reducer) {
		return key.hashCode() % reducer;
	}

}
