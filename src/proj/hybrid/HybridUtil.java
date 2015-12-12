package proj.hybrid;

import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class HybridUtil {

	public static MapWritable combine(MapWritable m1, MapWritable m2) {

		Iterator<Entry<Writable, Writable>> it = m2.entrySet().iterator();
		while (it.hasNext()) {

			Map.Entry<Writable, Writable> val = (Entry<Writable, Writable>) it
					.next();
			if (m1.containsKey(new Text(val.getKey().toString()))) {
				IntWritable temp = (IntWritable) m1.get(new Text(val.getKey().toString()));
				IntWritable temp1 = (IntWritable) val.getValue();
				m1.put(new Text(val.getKey().toString()), new IntWritable(temp.get() + temp1.get()));
			} else {
				m1.put(new Text(val.getKey().toString()), (IntWritable) val.getValue());
			}
		}
		return m1;
	}

	public static int getValueSum(MapWritable m1) {
		int total = 0;
		Iterator<Entry<Writable, Writable>> it = m1.entrySet().iterator();
		while (it.hasNext()) {
			Map.Entry<Writable, Writable> val = (Entry<Writable, Writable>) it
					.next();
			IntWritable temp = (IntWritable) val.getValue();
			total += temp.get();
		}
		return total;
	}

	public static MapWritable getRelativeFreq(MapWritable m1, int freq) {
		int count = 1;
		if (freq != 0) {
			count = freq;
		}
		Iterator<Entry<Writable, Writable>> it = m1.entrySet().iterator();
		while (it.hasNext()) {
			Map.Entry<Writable, Writable> val = (Entry<Writable, Writable>) it
					.next();
			DoubleWritable temp = (DoubleWritable) val.getValue();
			val.setValue(new DoubleWritable(temp.get() / (double) count));
		}
		return m1;
	}

	public static Text mapWritableToRelativeFreq(MapWritable m1, int total) {
		StringBuilder sb = new StringBuilder();
		NumberFormat formatter = new DecimalFormat("#0.00");
		sb.append("[ TOT-"+total);
		Iterator<Entry<Writable, Writable>> it = m1.entrySet().iterator();
		while (it.hasNext()) {
			MapWritable.Entry<Writable, Writable> entry = (MapWritable.Entry<Writable, Writable>) it
					.next();
			sb.append("(");
			sb.append(entry.getKey().toString());
			sb.append(", ");
			sb.append(formatter.format(((IntWritable) entry.getValue()).get() / (double) total));
			sb.append(")");
			sb.append("<");
			sb.append(((IntWritable) entry.getValue()).get());
			sb.append(">");
		}

		sb.append("]");
		return new Text(sb.toString());
	}
}
