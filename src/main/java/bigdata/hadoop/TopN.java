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
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public class TopN {
    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, IntWritable> {

        private Configuration conf;
        private BufferedReader fis;
        private List<Integer> topns;

        @Override
        public void setup(Context context) throws IOException,
                InterruptedException {
            conf = context.getConfiguration();
            topns = new ArrayList<Integer>();
        }

        @Override
        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String line = value.toString().trim();
            int b = Integer.parseInt(line);
            int i = 0;
            int length = topns.size();
            int index = 0;
            for (int j = 0; j < length; j++) {
                int c = topns.get(j);
                if (b > c) {
                    index = j;
                    break;
                }
            }
            topns.add(index, b);
            Text k = new Text("topn");
            IntWritable v = new IntWritable();
            v.set(b);
            context.write(k, v);
            length = topns.size();
            if (length > 5) {
                int p = length - 5;
                for (int g = 1; g <= p; g++) {
                    topns.remove(length-g);
                }
            }

        }
    }

    public static class IntSumReducer
            extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            int sum = 0;
            List<Integer> topn = new ArrayList<Integer>();
            for (IntWritable val : values) {
                topn.add(val.get());
            }
            Collections.sort(topn, new Comparator<Integer>() {
                @Override
                public int compare(Integer o1, Integer o2) {
                    return o2.compareTo(o1);
                }
            });
            for (int i = 0; i < topn.size() && i < 5; i++) {
                result.set(topn.get(i));
                context.write(new Text(i+""), result);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        GenericOptionsParser optionParser = new GenericOptionsParser(conf, args);
        String[] remainingArgs = optionParser.getRemainingArgs();
        if ((remainingArgs.length != 2) && (remainingArgs.length != 4)) {
            System.err.println("Usage: topn <in> <out> [-skip skipPatternFile]");
            System.exit(2);
        }
        Job job = Job.getInstance(conf, "top n");
        job.setJarByClass(TopN.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        List<String> otherArgs = new ArrayList<String>();
        for (int i = 0; i < remainingArgs.length; ++i) {
            otherArgs.add(remainingArgs[i]);
        }
        FileInputFormat.addInputPath(job, new Path(otherArgs.get(0)));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs.get(1)));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
