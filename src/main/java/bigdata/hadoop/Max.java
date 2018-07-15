package bigdata.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.BufferedReader;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public class Max {
    public static class MaxMapper
            extends Mapper<Object, Text, Text, Text> {

        private Configuration conf;
        private BufferedReader fis;
        private BigDecimal max;

        @Override
        public void setup(Context context) throws IOException,
                InterruptedException {
            conf = context.getConfiguration();
        }

        @Override
        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String line = value.toString().trim();
            BigDecimal b = new BigDecimal(line);
            int i = 0;
            if (max == null || b.compareTo(max) > 0) {
                max = b;
            }


        }

        @Override
        protected void cleanup(Context context
        ) throws IOException, InterruptedException {
            Text k = new Text("max");
            context.write(k, new Text(max.toString()));
        }
    }

    public static class MaxReducer
            extends Reducer<Text, Text, Text, Text> {
        private Text result = new Text();

        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            BigDecimal max = null;

            for (Text v:values) {
                BigDecimal v1= new BigDecimal(v.toString());
                if(max==null || v1.compareTo(max)>0)
                    max=v1;
            }
            result.set(max.toString());
            context.write(new Text("max"), result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        GenericOptionsParser optionParser = new GenericOptionsParser(conf, args);
        String[] remainingArgs = optionParser.getRemainingArgs();
        if ((remainingArgs.length != 2)) {
            System.exit(2);
        }
        Job job = Job.getInstance(conf, "max");
        job.setJarByClass(Max.class);
        job.setMapperClass(MaxMapper.class);
        job.setReducerClass(MaxReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        List<String> otherArgs = new ArrayList<String>();
        for (int i = 0; i < remainingArgs.length; ++i) {
            otherArgs.add(remainingArgs[i]);
        }
        FileInputFormat.addInputPath(job, new Path(otherArgs.get(0)));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs.get(1)));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
