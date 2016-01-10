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

public class Dbb {
	public static class Part_B_Mapper extends Mapper<Object, Text, Text, Text> {
		private static Player playerOne = new Player();
		private static Player playerTwo = new Player();
		private final static IntWritable one = new IntWritable(123456);
		private String sCurrentLine;

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			sCurrentLine = value.toString();
			String token;
			if ((token = getTagValue(sCurrentLine, TagStart + TagWhite)) != null) {
				playerOne.setPlayerId(token);
				playerOne.setPlayerColor("White");

			}
			if ((token = getTagValue(sCurrentLine, TagStart + TagBlack)) != null) {
				playerTwo.setPlayerId(token);
				playerTwo.setPlayerColor("Black");
			}
			if ((token = getTagValue(sCurrentLine, TagStart + TagResult)) != null) {
				if(getGameResult(token)==null ||getGameResult(token)=="")
					return;
				playerOne.setResult(token);
				playerTwo.setResult(token);
				context.write(new Text(playerOne.getPlayerNameColor()), new Text(playerOne.getPlayerGameCount()));
				context.write(new Text(playerTwo.getPlayerNameColor()), new Text(playerTwo.getPlayerGameCount()));				
playerOne = new Player();
				playerTwo = new Player();
			}
//				context.write(new Text(playerOne.toString()), one);
//				context.write(new Text(playerTwo.toString()), one);
		}
	}

	public static class Part_B_Combiner extends Reducer<Text,Text,Text,Text> {
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			int total = 0;
			int win = 0;
			int lost = 0;
			int draw = 0;
			for (Text val : values) {
				String value[] = val.toString().split(" ");
				context.write(key, new Text(val));
//				win = Integer.parseInt(value[0]);
//				lost = Integer.parseInt(value[1]);
//				draw = Integer.parseInt(value[2]);
//				total = total + win + lost + draw;
			}
			//context.write(key, new Text(Integer.toString(win) + " " + Integer.toString(lost) + " " + Integer.toString(draw) + " " + Integer.toString(total)));
		}
	}

	public static class Part_B_Reducer extends Reducer<Text,Text,Text,Text> {
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			int total = 0;
			int win = 0;
			int lost = 0;
			int draw = 0;
			/*for (Text val : values) {
				String value[] = val.toString().split(" ");
				context.write(key, new Text(val));
			}*/
			for (Text val : values) {
				String value[] = val.toString().split(" ");
				win += Integer.parseInt(value[0]);
				lost += Integer.parseInt(value[1]);
				draw += Integer.parseInt(value[2]);
			}
			total = win + lost + draw;
			context.write(key, new Text(Float.toString((float)win/total*100) + "%  " + Float.toString((float)lost/total*100) + "%  " + Float.toString((float)draw/total*100)+"%"));
		}
	}

	public static void main(String[] args) throws Exception {
	Configuration conf = new Configuration();

	Job job = Job.getInstance(conf, "Database 539 Part B");
	job.setJarByClass(Dbb.class);

	job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(Text.class);

	job.setMapperClass(Part_B_Mapper.class);
	job.setCombinerClass(Part_B_Combiner.class);
	job.setReducerClass(Part_B_Reducer.class);
	
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

}
