import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.*;


public class MutualFriends {


    public static class Map
            extends Mapper<LongWritable, Text, Text, Text> {

        Text person = new Text();
        Text frnds = new Text();

        public void map(LongWritable key, Text value, Context context
        ) throws IOException, InterruptedException {

            String[] split_str = value.toString().split("\\t");
            String user_id = split_str[0];
            if (split_str.length == 1) {
                return;
            }
            String[] frnds_l = split_str[1].split(",");
            for (String friend : frnds_l) {

                if (user_id.equals(friend))
                    continue;

                String userKey = (Integer.parseInt(user_id) < Integer.parseInt(friend)) ? user_id + ", " + friend : friend + ", " + user_id;
                String regex = "((\\b" + friend + "[^\\w]+)|\\b,?" + friend + "$)";
                frnds.set(split_str[1].replaceAll(regex, ""));
                person.set(userKey);
                context.write(person, frnds);
            }
        }

    }

    public static class Reduce
            extends Reducer<Text, Text, Text, Text> {

        private String findMatchingFriends(String list1, String list2) {

            if (list1 == null || list2 == null)
                return null;

            String[] frnd_l1 = list1.split(",");
            String[] frnd_l2 = list2.split(",");

            LinkedHashSet<String> set1 = new LinkedHashSet<>(Arrays.asList(frnd_l1));

            LinkedHashSet<String> set2 = new LinkedHashSet<>();
            Collections.addAll(set2, frnd_l2);

            set1.retainAll(set2);

            return set1.toString().replaceAll("[\\[\\]]", "");
        }

        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {

            String[] frnd_l = new String[2];
            int i = 0;

            for (Text value : values) {
                frnd_l[i++] = value.toString();
            }
            String mutualFriends = findMatchingFriends(frnd_l[0], frnd_l[1]);
            if (mutualFriends != null && mutualFriends.length() > 0) {
                mutualFriends = "\t" + mutualFriends;
                context.write(key, new Text(mutualFriends));
            }
        }

    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: MutualFriends <FriendsFile> <output>");
            System.exit(2);
        }

        Job job = Job.getInstance(conf, "MutualFriends");
        job.setJarByClass(MutualFriends.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
