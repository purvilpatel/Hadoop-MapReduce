import java.io.IOException;
import java.util.StringTokenizer;
import java.lang.Integer;
import java.lang.Float;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Dbc_PassOne {

	public static class Part_C_Mapper extends Mapper<Object, Text, Text, Text> {
		private String sCurrentLine;


		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			sCurrentLine = value.toString();
			String token;
			if ((token = getTagValue(sCurrentLine, TagStart + TagPlayerCount)) != null) {
				context.write(new Text(token), new Text(Integer.toString(1)));
				Dbc_PassOne.count++;
				context.write(new Text("#Count"), new Text(Integer.toString(Dbc_PassOne.count)));
			}
		}
	}

	public static class Part_C_Combiner extends Reducer<Text,Text,Text,Text> {
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			int total = 0;
			int max = 0;
			if(key.toString().equals("#Count")){
				for (Text val : values) {
					if(max < Integer.parseInt(val.toString()))
						max = Integer.parseInt(val.toString());
				}
				context.write(key, new Text(Integer.toString(max)));
			}
			else{
				for (Text val : values) {
					total += Integer.parseInt(val.toString());
				}
				context.write(key, new Text(Integer.toString(total)));
			}
		}
	}

	public static class Part_C_Reducer extends Reducer<Text,Text,Text,Text> {
		int maxCount = 0;
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			int total = 0;
			if(key.toString().equals("#Count")){
				for (Text val : values) {
					if(maxCount < Integer.parseInt(val.toString()))
						maxCount = Integer.parseInt(val.toString());
				}
			}
			else{
				for (Text val : values) {
					total += Integer.parseInt(val.toString());
				}
				context.write(key, new Text(Float.toString(((float)total)/maxCount*100)));
			}
		}
	}

	public static void main(String[] args) throws Exception {
	Configuration conf = new Configuration();

	Job job = Job.getInstance(conf, "Database 539 Part C Pass 1");
	job.setJarByClass(Dbc_PassOne.class);

	job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(Text.class);

	job.setMapperClass(Part_C_Mapper.class);
	job.setCombinerClass(Part_C_Combiner.class);
	job.setReducerClass(Part_C_Reducer.class);
	
	job.setInputFormatClass(TextInputFormat.class);
	job.setOutputFormatClass(TextOutputFormat.class);
	job.setNumReduceTasks(Integer.parseInt(args[2]));
	
	FileInputFormat.addInputPath(job, new Path(args[0]));
	FileOutputFormat.setOutputPath(job, new Path(args[1]));

	System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

	private static String getTagValue(String text, String tag) {
		if (text.startsWith(tag)) {
			int startIdx = text.indexOf("\"");
			int endIdx = text.lastIndexOf("\"");
			return text.substring(startIdx + 1, endIdx);
		}
		return null;
	}

	private static String getGameResult(String text) {
		switch (text) {
		case BLACK_WON:
			return "Black";
		case WHITE_WON:
			return "White";
		case GAME_DRAW:
			return "Draw";
		}
		return null;
	}

	private final static String TagStart = "[";
	private final static String TagEnd = "]";
	private final static String TagWhite = "White ";
	private final static String TagBlack = "Black ";
	private final static String TagPlayerCount = "PlyCount ";
	private final static String TagResult = "Result ";

	private final static String WHITE_WON = "1-0";
	private final static String BLACK_WON = "0-1";
	private final static String GAME_DRAW = "1/2-1/2";
	public static int count = 0 ;
}
