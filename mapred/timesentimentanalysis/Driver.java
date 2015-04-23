package mapred.timesentimentanalysis;

import java.io.IOException;
import mapred.job.Optimizedjob;
import mapred.util.SimpleParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;

public class Driver {

	public static void main(String args[]) throws Exception {
		SimpleParser parser = new SimpleParser(args);

		String input = parser.get("input");
		String output = parser.get("output");

		getJobFeatureVector(input, output);

	}

	private static void getJobFeatureVector(String input, String output)
			throws IOException, ClassNotFoundException, InterruptedException {
		Optimizedjob job = new Optimizedjob(new Configuration(), input, output,
				"Compute time sentiment");

		job.setClasses(SentimentTimeMapper.class, SentimentTimeReducer.class, null);
		job.setMapOutputClasses(Text.class, Text.class);

		job.run();
	}	
}
