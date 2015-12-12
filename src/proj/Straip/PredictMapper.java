package proj.Straip;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Pattern;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class PredictMapper extends
		Mapper<LongWritable, Text, Text, MapWritable> {

	Map<Text, MapWritable> mwGlobal;
	private final Pattern newLineSeprate = Pattern.compile("//.*\n");

	@Override
	public void setup(Context context) {
		mwGlobal = new HashMap<Text, MapWritable>();
	}

	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		String[] inputLines = newLineSeprate.split(value.toString());

		for (int i = 0; i < inputLines.length; i++) {

			String[] userVal = inputLines[i].split("\\s");

			for (int j = 0; j < userVal.length - 1; j++) {

				MapWritable straip = new MapWritable();

				for (int k = j + 1; k < userVal.length; k++) {

					if (userVal[j].equalsIgnoreCase(userVal[k])) {
						break;
					}
					if (straip.containsKey(new Text(userVal[k]))) {
						IntWritable temp = (IntWritable) straip.get(new Text(userVal[k]));
						straip.put(new Text(userVal[k]),
								new IntWritable(temp.get() + 1));
					} else {
						straip.put(new Text(userVal[k]), new IntWritable(1));
					}
				}
				if (mwGlobal.containsKey(new Text(userVal[j]))) {
					MapWritable tempMap = StraipUtil.combine(mwGlobal.get(new Text(userVal[j])),
							straip);
					mwGlobal.put(new Text(userVal[j]), tempMap);
				} else {
					mwGlobal.put(new Text(userVal[j]), straip);
				}

			}

		}

	}

	@Override
	protected void cleanup(Context context) throws IOException,
			InterruptedException {
		Iterator<Entry<Text, MapWritable>> it = mwGlobal.entrySet().iterator();
		while (it.hasNext()) {
			Map.Entry<Text, MapWritable> iterItem = (Map.Entry<Text, MapWritable>) it
					.next();
			context.write(iterItem.getKey(), iterItem.getValue());
		}
	}
}
