package mapred.hashtagsim;

import java.io.IOException;
import java.util.TreeMap;
import java.util.Map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SimilarityAllMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	/**
	 * We emit the pairs with
	 */
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
		String[] hashtag_featureVector = line.split("\\s+", 2);

		String hashtag = hashtag_featureVector[0];
		Map<String, Integer> features = parseFeatureVector(hashtag_featureVector[1]);
        String [] hashtags = new String[features.size()];
        
        int index = 0;
		for (String word : features.keySet())
            hashtags[index++] = word;
		    
        for (int i = 0; i < features.size(); i ++)
            for (int j = i + 1; j < features.size(); j ++)
                context.write(new Text(hashtags[i] + "\t" + hashtags[j]), new IntWritable(features.get(hashtags[i]) * features.get(hashtags[j])));
	}

	/**
	 * De-serialize the feature vector into a map
	 * 
	 * @param featureVector
	 *            The format is "word1:count1;word2:count2;...;wordN:countN;"
	 * @return A HashMap, with key being each word and value being the count.
	 */
	private Map<String, Integer> parseFeatureVector(String featureVector) {
		Map<String, Integer> featureMap = new TreeMap<String, Integer>();
		String[] features = featureVector.split(";");
		for (String feature : features) {
			String[] word_count = feature.split(":");
			featureMap.put(word_count[0], Integer.parseInt(word_count[1]));
		}
		return featureMap;
	}
}














