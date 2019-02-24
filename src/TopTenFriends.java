import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

public class TopTenFriends extends Configured implements Tool {
    public static class Map2 extends Mapper<LongWritable, Text, IntWritable, Text> {

        IntWritable k = new IntWritable();
        Text v = new Text();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] line = value.toString().split("\\t\\t");
            String[] frnds = line[0].split(", ");
            String Friend_pair = frnds[0] + "\t" + frnds[1];

            int count = 0;
            if (line.length == 2) {
                ArrayList<String> FriendsList = new ArrayList<>(Arrays.asList(line[1].split(", ")));
                count = FriendsList.size();
                v.set(Friend_pair + "\t" + FriendsList);
            } else {
                v.set(Friend_pair);
            }
            k.set(count);
            context.write(k, v);

        }
    }

    public static class Reduce2 extends Reducer<IntWritable, Text, Text, Text> {
        Text v = new Text();
        Text k = new Text();
        private int print_count = 0;

        public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text value : values) {
                String out = value.toString();
                if (print_count == 10) {
                    break;
                } else {
                    String[] splitter = out.split("\\t");
                    v.set("\t" + key.get() + "\t" + splitter[2]);
                    k.set(splitter[0] + ", " + splitter[1]);
                    context.write(k, v);
                    print_count++;
                }

            }
        }
    }

    public static class customComparator extends WritableComparator {
        public customComparator() {
            super(IntWritable.class, true);
        }

        @Override
        public int compare(WritableComparable w1, WritableComparable w2) {
            IntWritable key1 = (IntWritable) w1;
            IntWritable key2 = (IntWritable) w2;
            return -1 * key1.compareTo(key2);
        }
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new TopTenFriends(), args);
        System.exit(exitCode);
    }


    public int run(String[] args) throws Exception {

        Configuration conf1 = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf1, args).getRemainingArgs();
        // get all args
        if (otherArgs.length != 2) {
            System.out.println(otherArgs[0]);
            System.err.println("Usage: Top10MutualFriends <in> <out>");
            System.exit(2);
        }

        Job job2 = Job.getInstance(conf1, "Top10MutualFriends");
        job2.setJobName("Top 10 Mutual Friends Second Job");

        FileInputFormat.setInputPaths(job2, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job2, new Path(otherArgs[1] + "/final"));

        job2.setSortComparatorClass(customComparator.class);
        job2.setMapperClass(Map2.class);
        job2.setReducerClass(Reduce2.class);


        job2.setOutputKeyClass(IntWritable.class);
        job2.setOutputValueClass(Text.class);

        // delete temp directory
        if (job2.waitForCompletion(true)) {
            return 1;
        } else {
            return 0;
        }

    }

}

