package com.hadoop.examples.anagrams; 
import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer; 
import org.apache.hadoop.io.Text; 
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
public class Anagrams {
 public class Mapper extends MapReduceBase implements
		Mapper<LongWritable, Text, Text, Text> {
	private Text sortedText = new Text();
	private Text orginalText = new Text();	
	public void map(LongWritable key, Text value,
			OutputCollector<Text, Text> outputCollector, Reporter reporter)
			throws IOException {
		String word = value.toString();
		char[] wordChars = word.toCharArray();
		Arrays.sort(wordChars);
		String sortedWord = new String(wordChars);
		sortedText.set(sortedWord);
		originalText.set(word);
		outputCollector.collect(sortedText, originalText);
	}
}
public class Reducer extends MapReduceBase implements Reducer<Text, Text, Text, Text> { 
        private Text outputKey = new Text();
        private Text outputValue = new Text();        
        public void reduce(Text anagramKey, Iterator<Text> anagramValues,
                        OutputCollector<Text, Text> results, Reporter reporter) throws IOException {
                String output = "";
                while(anagramValues.hasNext())
 
                {
                        Text anagam = anagramValues.next();
                        output = output + anagam.toString() + "-";
                }
                StringTokenizer outputTokenizer = new StringTokenizer(output,"-");
                if(outputTokenizer.countTokens()>=2)
                {
                        output = output.replace("-", ",");
 
                        outputKey.set(anagramKey.toString());
 
                        outputValue.set(output);
 
                        results.collect(outputKey, outputValue);
                }
 
        }
 
} 

 public static void main(String[] args) throws Exception {
 Configuration conf = new Configuration();
      
    Job job = new Job(conf, "anagrams");
   job.setOutputKeyClass(Text.class);
  job.setOutputValueClass(IntWritable.class);      
  job.setMapperClass(Mappper.class);
  job.setReducerClass(Reducer.class);        
  job.setInputFormatClass(TextInputFormat.class);
  job.setOutputFormatClass(TextOutputFormat.class);         
  FileInputFormat.addInputPath(job, new Path(args[0]));
  FileOutputFormat.setOutputPath(job, new Path(args[1]));         
  job.waitForCompletion(true);
   }         
}

