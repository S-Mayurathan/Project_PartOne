package proj.hybrid;

import java.io.IOException;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import proj.hybrid.Pair;

public class PredictReducer extends Reducer<Pair, IntWritable, Text, Text> {

	int total = 0;
	Map<String, Double> gloal = new HashMap<String, Double>();
	String currentTerm = null;

	@Override
	public void reduce(Pair key, Iterable<IntWritable> counts, Context context)
			throws IOException, InterruptedException {

		if (currentTerm == null)
			currentTerm = key.first.toString();
		else if (!currentTerm.equals(key.first.toString())) {
			Iterator<Entry<String, Double>> it = gloal.entrySet().iterator();
			while (it.hasNext()) {
				Map.Entry<String, Double> entry = (Map.Entry<String, Double>) it
						.next();
				gloal.put(entry.getKey().toString(), entry.getValue().intValue()
						/ (double) total);
			}

			writeContex(context, currentTerm, gloal);

			total = 0;
			gloal = new HashMap<String, Double>();
			currentTerm = key.first.toString();
		}

		for (IntWritable c : counts) {
			if (gloal.get(key.second) == null) {
				gloal.put(key.second.toString(), new Double(c.get()));
			} else {
				Double dw = (Double) gloal.get(key.second.toString());
				gloal.put(key.second.toString(), new Double(dw + c.get()));
			}
			total += c.get();
		}
	}

	@Override
	protected void cleanup(Context context) throws IOException,
			InterruptedException {

		Iterator<Entry<String, Double>> it = gloal.entrySet().iterator();
		while (it.hasNext()) {
			Map.Entry<String, Double> entry = (Map.Entry<String, Double>) it
					.next();
			gloal.put(entry.getKey().toString(), entry.getValue().intValue()
					/ (double) total);
		}

		writeContex(context, currentTerm, gloal);
	}

	private void writeContex(Context context, String term, Map<String, Double> hm)
			throws IOException, InterruptedException {
		StringBuilder sb = new StringBuilder();
		NumberFormat formatter = new DecimalFormat("#0.00");
		sb.append("[");

		Iterator<Entry<String, Double>> it = hm.entrySet().iterator();
		while (it.hasNext()) {
			Map.Entry<String, Double> entry = (Map.Entry<String, Double>) it
					.next();
			sb.append("(");
			sb.append(entry.getKey().toString());
			sb.append(", ");
			sb.append(formatter.format(entry.getValue()));
			sb.append(")");
		}

		sb.append("]");
		context.write(new Text(term), new Text(sb.toString()));
	}
}