package mapred.localsentimentanalysis;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SentimentLocationMapper extends Mapper<LongWritable, Text, Text, Text> {
	
	Map<String, Coordinate> cityMap;
	@Override
	protected void setup(Context context) {
		this.cityMap = new HashMap<String, Coordinate>();
		cityMap.put("Pittsburgh", new Coordinate(-80.0, 40.44));
		cityMap.put("San Francisco", new Coordinate(-122.44, 37.71));
		cityMap.put("New York", new Coordinate(-73.97, 40.75));
		cityMap.put("Houston", new Coordinate(-95.36, 29.75));
		cityMap.put("Chicago", new Coordinate(-87.78, 41.83));
		cityMap.put("Miami", new Coordinate(-80.24, 25.93));
		cityMap.put("London", new Coordinate(-0.085, 51.5));
	}

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
		String[] fields = line.split("\t");
		//System.out.println(line);
		String tweet = fields[fields.length - 1];
		String coordStr = fields[4];
		if (!coordStr.isEmpty()) {
			Coordinate coord = new Coordinate(Double.parseDouble(coordStr.split(",")[0]), Double.parseDouble(coordStr.split(",")[1]));
			context.write(new Text(coord.getClosestCityName(this.cityMap)), new Text(tweet));
		}
	}
}
