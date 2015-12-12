package proj.pair;

import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class PredictReducer extends
		Reducer<Pair, IntWritable, Pair, DoubleWritable> {

	int total;

	@Override
	public void reduce(Pair key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {

		if (key.second.toString().equals("*")) {
			total = 0;
			for (IntWritable c : values) {
				total += c.get();
			}
		} else {
			int sum = 0;
			for (IntWritable c : values) {
				sum += c.get();
			}
			context.write(key, new DoubleWritable(sum / (double) total));
		}
	}

}