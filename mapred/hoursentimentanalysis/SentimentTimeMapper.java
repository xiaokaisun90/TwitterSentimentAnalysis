package mapred.hoursentimentanalysis;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Date;
import java.util.TimeZone;
import java.text.SimpleDateFormat;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SentimentTimeMapper extends Mapper<LongWritable, Text, Text, Text> {
	private Text word = new Text();

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
		String[] fields = line.split("\t");
		//System.out.println(line);
		String tweet = fields[fields.length - 1];
		String date = fields[2];
		long timeNum = Long.parseLong(date);
		String realTime = convert(timeNum);
		String[] timeSplit = realTime.split(" ");
		word.set(timeSplit[0]);
		// word.set(realTime);
		context.write(word, new Text(tweet));

	}
	public static String convert(long unix) {
		Date date = new Date(unix); // *1000 is to convert seconds to milliseconds
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss z"); // the format of your date
		sdf.setTimeZone(TimeZone.getTimeZone("GMT-4")); // give a timezone reference for formating (see comment at the bottom
		String formattedDate = sdf.format(date);
		return formattedDate;
	}
}
