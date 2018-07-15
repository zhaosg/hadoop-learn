package bigdata.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
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
import java.util.List;

public class Avg {
    public static class AvgMapper
            extends Mapper<Object, Text, Text, Text> {

        private Configuration conf;
        private BufferedReader fis;
        private BigDecimal sum;
        private long count = 0;

        @Override
        public void setup(Context context) throws IOException,
                InterruptedException {
            conf = context.getConfiguration();
            sum = new BigDecimal(0);
        }

        @Override
        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String line = value.toString().trim();
            if (line.length() > 0) {
                count++;
                sum = sum.add(new BigDecimal(line));
            }
        }

        @Override
        protected void cleanup(Context context
        ) throws IOException, InterruptedException {
            context.write(new Text("sumcount"), new Text(sum.toString() + "," + count));
        }
    }

    public static class AvgReducer
            extends Reducer<Text, Text, Text, Text> {
        private Text result = new Text();

        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            BigDecimal sum = new BigDecimal(0);
            long count = 0;
            for (Text v : values) {
                String line = v.toString().trim();
                String[] ss = line.split(",");

                sum = sum.add(new BigDecimal(ss[0]));
                count += Long.valueOf(ss[1]);
            }
            BigDecimal avg = sum.divide(new BigDecimal(count),7,BigDecimal.ROUND_HALF_UP);
            result.set(avg.toString());
            context.write(new Text("avg"), result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        GenericOptionsParser optionParser = new GenericOptionsParser(conf, args);
        String[] remainingArgs = optionParser.getRemainingArgs();
        if ((remainingArgs.length != 2)) {
            System.exit(2);
        }
        Job job = Job.getInstance(conf, "avg");
        job.setJarByClass(Avg.class);
        job.setMapperClass(AvgMapper.class);
        job.setReducerClass(AvgReducer.class);
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
