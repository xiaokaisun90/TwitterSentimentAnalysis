package mapred.hoursentimentanalysis;

import java.util.HashMap;
import java.util.Map;

import java.text.SimpleDateFormat;
import java.io.IOException;
import java.util.Properties;
import java.util.Date;
import java.util.TimeZone;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.neural.rnn.RNNCoreAnnotations;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations;
import edu.stanford.nlp.trees.Tree;
import edu.stanford.nlp.util.CoreMap;

public class SentimentTimeReducer extends Reducer<Text, Text, Text, DoubleWritable> {

	StanfordCoreNLP pipeline;
	@Override
	protected void setup(Context context) {
		Properties prop = new Properties();
		prop.setProperty("annotators", "tokenize, ssplit, parse, sentiment");
		pipeline = new StanfordCoreNLP(prop);
	}
	@Override
	protected void reduce(Text key, Iterable<Text> value,
			Context context)
			throws IOException, InterruptedException {
			Map<String,Integer> map = new HashMap<String, Integer>();
			int count = 0;
			double timeSentiment = 0.0;
			for (Text tweet : value) {
				timeSentiment += this.findSentiment(tweet.toString());
				count += 1;			
		}
		context.write(key, new DoubleWritable(count));
		context.write(key, new DoubleWritable(timeSentiment/count));



	}

public static int findSentiment(String tweet) {

         try {
         	 	Properties props = new Properties();
         	    props.setProperty("annotators", "tokenize, ssplit, parse, sentiment");
         	    StanfordCoreNLP pipeline = new StanfordCoreNLP(props);
 		        	int mainSentiment = 0;
 		        if (tweet != null && tweet.length() > 0) {
 		            int longest = 0;
 		            Annotation annotation = pipeline.process(tweet);
// 		            System.out.println(annotation);
 		            for (CoreMap sentence : annotation
 		                    .get(CoreAnnotations.SentencesAnnotation.class)) {
 		                Tree tree = sentence
 		                        .get(SentimentCoreAnnotations.AnnotatedTree.class);
 		                int sentiment = RNNCoreAnnotations.getPredictedClass(tree);
 		                String partText = sentence.toString();
 		                if (partText.length() > longest) {
 		                    mainSentiment = sentiment;
 		                    longest = partText.length();
 		                }
 		
 		            }
 		        }
 		        return mainSentiment;
         } catch(NullPointerException e) {
         	System.out.println("nullpointerexception");
         	return 0;
         }
 		
     }
}
