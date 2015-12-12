package proj.Straip;

import java.io.IOException;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class PredictReducer extends
		Reducer<Text, MapWritable, Text, Text> {

	@Override
	public void reduce(Text key, Iterable<MapWritable> values, Context context)
			throws IOException, InterruptedException {
		int total=0;
		MapWritable temp=new MapWritable();
		for(MapWritable straip:values){
			total+=StraipUtil.getValueSum(straip);
			temp=StraipUtil.combine(temp, straip);
		}
		context.write(key, StraipUtil.mapWritableToRelativeFreq(temp,total));
	}

}