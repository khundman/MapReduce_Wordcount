import java.io.*;
import java.util.*;
import org.apache.commons.lang.StringUtils;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapred.lib.MultipleInputs;

public class GoogleBooks extends Configured implements Tool { 

    public static class Mapper1 extends MapReduceBase implements Mapper<LongWritable, Text, Text, DoubleWritable> {

    DoubleWritable volumes = new DoubleWritable();
	Text yearSubstring = new Text(); 

	public void configure(JobConf job) {
	}

		public void map(LongWritable key, Text value, OutputCollector<Text, DoubleWritable> output, Reporter reporter) throws IOException {
		    String[] tokens = value.toString().split("\\t");
			    if (tokens[0].contains("nu")) { 
			    	if (StringUtils.isNumeric(tokens[1])){
			    		yearSubstring.set(tokens[1] + " nu");
			    		volumes.set(Integer.parseInt(tokens[3].trim()));
						output.collect(yearSubstring, volumes);
					}
			    }
			    if (tokens[0].contains("die")) { 
			    	if (StringUtils.isNumeric(tokens[1])){
			    		yearSubstring.set(tokens[1] + " die");
			    		volumes.set(Integer.parseInt(tokens[3].trim()));
						output.collect(yearSubstring, volumes);
					}
			    }
			    if (tokens[0].contains("kla")) { 
			    	if (StringUtils.isNumeric(tokens[1])){
			    		yearSubstring.set(tokens[1] + " kla");
			    		volumes.set(Integer.parseInt(tokens[3].trim()));
						output.collect(yearSubstring, volumes);
					}
			    }
			}
		}
		
	public static class Mapper2 extends MapReduceBase implements Mapper<LongWritable, Text, Text, DoubleWritable> {

		DoubleWritable volumes = new DoubleWritable();
		Text yearSubstring = new Text(); 

		public void configure(JobConf job) {
		}

		public void map(LongWritable key, Text value, OutputCollector<Text, DoubleWritable> output, Reporter reporter) throws IOException {
		    String[] tokens = value.toString().split("\\s+|\\t+");
			    if (tokens[0].contains("nu") || tokens[1].contains("nu")) { 
			    	if (StringUtils.isNumeric(tokens[2])){
			    		yearSubstring.set(tokens[2] + " nu");
			    		volumes.set(Integer.parseInt(tokens[4].trim()));
						output.collect(yearSubstring, volumes);
					}
			    }
			    if (tokens[0].contains("die") || tokens[1].contains("die")) { 
			    	if (StringUtils.isNumeric(tokens[2])){
			    		yearSubstring.set(tokens[2] + " die");
			    		volumes.set(Integer.parseInt(tokens[4].trim()));
						output.collect(yearSubstring, volumes);
					}
			    }
			    if (tokens[0].contains("kla") || tokens[1].contains("kla")) { 
			    	if (StringUtils.isNumeric(tokens[2])){
			    		yearSubstring.set(tokens[2] + " kla");
			    		volumes.set(Integer.parseInt(tokens[4].trim()));
						output.collect(yearSubstring, volumes);
					}
			    }
			}
	   }

    public static class Reduce extends MapReduceBase implements Reducer<Text, DoubleWritable, Text, DoubleWritable> {
		public void reduce(Text key, Iterator<DoubleWritable> values, OutputCollector<Text, DoubleWritable> output, Reporter reporter) throws IOException {
		    double sum = 0;
		    int count = 0;
		    double avg = 0;
		    while (values.hasNext()) {
		    	sum += values.next().get();
		    	count += 1;
			}
			avg = sum / count;
		    output.collect(key, new DoubleWritable(avg));
		}
    }

    public int run(String[] args) throws Exception {
	JobConf conf = new JobConf(getConf(), GoogleBooks.class);
	conf.setJobName("GoogleBooks");

	conf.setOutputKeyClass(Text.class);
	conf.setOutputValueClass(DoubleWritable.class);

	//conf.setMapperClass(Map.class);
	//conf.setCombinerClass(Reduce.class);
	conf.setReducerClass(Reduce.class);

	conf.setInputFormat(TextInputFormat.class);
	conf.setOutputFormat(TextOutputFormat.class);

	//FileInputFormat.setInputPaths(conf, new Path(args[0]));
	//FileInputFormat.setInputPaths(conf, new Path(args[1]));
	
	MultipleInputs.addInputPath(conf, new Path(args[0]), TextInputFormat.class, Mapper1.class);
	MultipleInputs.addInputPath(conf, new Path(args[1]), TextInputFormat.class, Mapper2.class);
	FileOutputFormat.setOutputPath(conf, new Path(args[2]));

	JobClient.runJob(conf);
	return 0;
    }

    public static void main(String[] args) throws Exception {
	int res = ToolRunner.run(new Configuration(), new GoogleBooks(), args);
	System.exit(res);
    }
}



