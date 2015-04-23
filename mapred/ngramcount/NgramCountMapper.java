package mapred.ngramcount;

import java.io.IOException;

import mapred.util.Tokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class NgramCountMapper extends Mapper<LongWritable, Text, Text, NullWritable> {


    int n = 1;
	@Override
	protected void setup(Context context) {
        n = context.getConfiguration().getInt("ngram", 1) ;
	}

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
		String[] words = Tokenizer.tokenize(line);

		for (int i = 0; i < words.length - n + 1; i++) {
            String gram = words[i]; 

            for (int j = 1; j < n;  j++)
			    gram += " " + words[i + j];

            context.write(new Text(gram), NullWritable.get());
        }

	}
}
