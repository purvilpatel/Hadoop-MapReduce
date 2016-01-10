import java.io.IOException;
import java.util.StringTokenizer;

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

public class Dba {

	public static class Part_A_Mapper extends Mapper<Object, Text, Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();
		private String sCurrentLine;

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			sCurrentLine = value.toString();
			String token;
			if ((token = getTagValue(sCurrentLine, TagBeg + TagResult)) != null) {
				word.set(getGameResult(token));
				context.write(word, one);
			}
		}
	}

	public static class Part_A_Reducer extends Reducer<Text,IntWritable,Text,IntWritable> {
		private int count = 0;
		private int black = 0;
		private int white = 0;
		private int draw = 0;
		private int cumulativeTotal = 0;
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			if(key.toString().compareToIgnoreCase("Black") == 0){
				black = sum;
				cumulativeTotal += black;
			}
			if(key.toString().compareToIgnoreCase("White") == 0){
				white = sum;
				cumulativeTotal += white;
			}
			if(key.toString().compareToIgnoreCase("Draw") == 0){
				draw = sum;
				cumulativeTotal += draw;
			}

			count++;
			if(count == 3){
			context.write(new Text("Black " + Float.toString((float)(black)/cumulativeTotal*100)+  "% "), new IntWritable(black));
			context.write(new Text("Draw " +  Float.toString((float)(draw)/cumulativeTotal*100)+ "% "), new IntWritable(draw));
			context.write(new Text("White " + Float.toString((float)(white)/cumulativeTotal*100)+  "% "), new IntWritable(white));
//			context.write(new Text("count " +  "% "), new IntWritable(count));
//			context.write(new Text("total " +  "% "), new IntWritable(cumulativeTotal));
			}
 			//context.write(key, result);
			//context.write(key, new IntWritable(count));
		}
	}

	public static void main(String[] args) throws Exception {
	Configuration conf = new Configuration();

	Job job = Job.getInstance(conf, "Database 539 Part A");
	job.setJarByClass(Dba.class);

	job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(IntWritable.class);

	job.setMapperClass(Part_A_Mapper.class);
	//job.setCombinerClass(Part_A_Reducer.class);
	job.setNumReduceTasks(Integer.parseInt(args[2]));
	job.setReducerClass(Part_A_Reducer.class);
	
	job.setInputFormatClass(TextInputFormat.class);
	job.setOutputFormatClass(TextOutputFormat.class);
	
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

	private final static String TagBeg = "[";
	private final static String TagEnd = "]";
	private final static String TagWhite = "White ";
	private final static String TagBlack = "Black ";
	private final static String TagPlayerCount = "PlyCount ";
	private final static String TagResult = "Result ";

	private final static String WHITE_WON = "1-0";
	private final static String BLACK_WON = "0-1";
	private final static String GAME_DRAW = "1/2-1/2";

}
