package proj.pair;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Pattern;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class PredictMapper extends
		Mapper<LongWritable, Text, Pair, IntWritable> {

	private Map<Pair, Integer> hm;
	private final Pattern newLineSeprate = Pattern.compile("//.*\n");

	@Override
	public void setup(Context context) {
		hm = new HashMap<Pair, Integer>();
	}

	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String[] inputLines = newLineSeprate.split(value.toString());
		for (int i = 0; i < inputLines.length; i++) {
			String[] userVal = inputLines[i].split("\\s");
			for (int j = 0; j < userVal.length - 1; j++) {

				Pair totalKey = new Pair(userVal[j], "*");

				for (int k = j + 1; k < userVal.length; k++) {
					if (userVal[j].equalsIgnoreCase(userVal[k])) {
						break;
					}
					Pair userkey = new Pair(userVal[j], userVal[k]);
					if (hm.containsKey(userkey)) {
						int temp = hm.get(userkey) + 1;
						hm.put(userkey, temp);
					} else {
						hm.put(userkey, 1);
					}

					if (hm.containsKey(totalKey)) {
						int tempt = hm.get(totalKey) + 1;
						hm.put(totalKey, tempt);
					} else {
						hm.put(totalKey, 1);
					}
				}
			}

		}

	}

	@Override
	protected void cleanup(Context context) throws IOException,
			InterruptedException {
		Iterator<Entry<Pair,Integer>> it=hm.entrySet().iterator();
		while(it.hasNext()){
			Map.Entry<Pair, Integer> iterItem=(Map.Entry<Pair, Integer>) it.next();
			context.write(iterItem.getKey(), new IntWritable(iterItem.getValue()));
		}
	}
}
