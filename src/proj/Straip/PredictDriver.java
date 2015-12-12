package proj.Straip;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.fs.Path;

public class PredictDriver {

  public static void main(String[] args) throws Exception {
	  
	    Configuration conf = new Configuration();
	    Job job = Job.getInstance(conf, "Pair");
	    job.setJarByClass(PredictDriver.class);
	    job.setMapperClass(PredictMapper.class);
	    job.setPartitionerClass(PredictPartitioner.class);
	    job.setReducerClass(PredictReducer.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(MapWritable.class);
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}

