import java.io.IOException;
import java.util.StringTokenizer;
import java.util.*;
import java.nio.*;

import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.*;
import org.apache.hadoop.io.Text.Comparator;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Dbc_PassTwo {


	public static class Pass_Two_Mapper
       extends Mapper<Object, Text, Text, Text>{

    private final static FloatWritable one = new FloatWritable(1);
	String freq = "";
	String perc = "";
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString());
      while (itr.hasMoreTokens()) {
		freq = itr.nextToken();
		perc = itr.nextToken();
      }
        context.write( new Text(perc), new Text(freq));
    }
  }


	public static class FloatComp extends WritableComparator{
	public FloatComp(){
		super(FloatWritable.class);
	}

	@Override
	public int compare(byte[] text1, int start1, int length1, byte[] text2, int start2, int length2){
		Float v1 = ByteBuffer.wrap(text1,start1,length1).getFloat();
		Float v2 = ByteBuffer.wrap(text2,start2,length2).getFloat();

		return v1.compareTo(v2) * (-1);
	}
}

	public static class StrComp extends WritableComparator{
	public StrComp(){
		super(Text.class);
	}
	
	static final Text.Comparator TEXT = new Text.Comparator();

	@Override
	public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2){
	try{
		int firstL1 = WritableUtils.decodeVIntSize(b1[s1]) + readVInt(b1,s1);
		int firstL2 = WritableUtils.decodeVIntSize(b2[s2]) + readVInt(b2,s2);
		String str1 = Text.decode(b1,s1,firstL1);
		String str2 = Text.decode(b2,s2,firstL2);
		Float v1 = new Float(str1);
		Float v2 = new Float(str2);

		return v1.compareTo(v2) * (-1);
	}
	catch(Exception e){
		return 0;
	}
	}

}

	public static class Part_C_Partitioner extends Partitioner<Text, Text> {
 	        @Override
		public int getPartition(Text key, Text value, int numReduceTasks) {
		float freq = Float.parseFloat(key.toString());
		float partitionRange = 2.0f / numReduceTasks;
		
		for (int i = 1; i <= numReduceTasks; i++) {
			if (freq > (i - 1) * partitionRange && freq <= (i) * partitionRange)
				return Math.abs(i - numReduceTasks);
		}
		return 0;
		}
	}
  public static class Pass_Two_Reducer
       extends Reducer<Text, Text,Text ,Text> {

    public void reduce(Text K, Iterable<Text> V,
                       Context context) throws IOException, InterruptedException {
	ArrayList<String> _values = new ArrayList<String>();
	String _key = K.toString();
	for (Text val : V) {
		String _val = val.toString();
		_values.add(_val);
		Text t1 = new Text(_val);
		Text t2 = new Text(_key);
		context.write(t1,t2);
	}
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();

    Job job = Job.getInstance(conf, "Database 539 Part C Pass 2");
    job.setJarByClass(Dbc_PassTwo.class);

    job.setSortComparatorClass(StrComp.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    job.setPartitionerClass(Part_C_Partitioner.class);

    job.setMapperClass(Pass_Two_Mapper.class);
    job.setCombinerClass(Pass_Two_Reducer.class);
    job.setReducerClass(Pass_Two_Reducer.class);
    job.setNumReduceTasks(Integer.parseInt(args[2]));

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
