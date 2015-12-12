package proj.pair;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class Pair implements WritableComparable<Pair> {

	Text first;
	Text second;

	public Pair() {
		this.first = new Text();
		this.second = new Text();
	}

	public Pair(Text first, Text secound) {
		super();
		this.first = first;
		this.second = secound;
	}
	
	public Pair(String first, String second){
		super();
		this.first = new Text(first);
		this.second = new Text(second);
	}

	@Override
	public String toString() {
		return "(" + first + ", " + second + ")";
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((first == null) ? 0 : first.hashCode());
		result = prime * result + ((second == null) ? 0 : second.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Pair other = (Pair) obj;
		if (first == null) {
			if (other.first != null)
				return false;
		} else if (!first.equals(other.first))
			return false;
		if (second == null) {
			if (other.second != null)
				return false;
		} else if (!second.equals(other.second))
			return false;
		return true;
	}

	@Override
	public void readFields(DataInput input) throws IOException {
		first.readFields(input);
		second.readFields(input);

	}

	@Override
	public void write(DataOutput out) throws IOException {
		first.write(out);
		second.write(out);

	}

	@Override
	public int compareTo(Pair pair) {
		int cmp = first.compareTo(pair.first);
		if (cmp != 0)
			return cmp;
		if ("*".equals(this.second.toString()) )
			return -1;
		else if ("*".equals(pair.second.toString()) )
			return 1;
		return second.compareTo(pair.second);
	}

}
