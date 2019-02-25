import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;

public class InMemoryJoin {
    private static HashMap<String, String> storeMap;

    public static class Map extends Mapper<Text, Text, Text, Text> {
        String friendData;

        public void setup(Context context) throws IOException {
            Configuration config = context.getConfiguration();
            storeMap = new HashMap<>();
            String userdataPath = config.get("userdata");
            Path path = new Path(userdataPath);
            FileSystem fs = FileSystem.get(config);
            BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path)));
            String line;
            line = br.readLine();
            while (line != null) {
                String[] Arr = line.split(",");
                if (Arr.length == 10) {
                    String data = " " + Arr[1] + ": " + Arr[4];
                    storeMap.put(Arr[0].trim(), data);
                }
                line = br.readLine();
            }
        }

        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] split = line.split(",");
            if (null != storeMap && !storeMap.isEmpty()) {
                for (String s : split) {
                    if (storeMap.containsKey(s)) {
                        friendData = storeMap.get(s);
                        storeMap.remove(s);
                        context.write(key, new Text(friendData));
                    }
                }
            }
        }
    }

    public static class Reduce extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Text counter = new Text();
            StringBuilder s = new StringBuilder();
            for (Text t : values) {
                if (s.toString().equals(""))
                    s = new StringBuilder("[");
                s.append(t).append(",");
            }
            s = new StringBuilder(s.substring(0, s.length() - 1));
            s.append("]");
            counter.set(s.toString());
            context.write(key, counter);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 5) {
            System.err.println("Usage: InMemoryJoin UserA UserB <FriendsFile> <UserData> <Output>");
            System.exit(2);
        }
        conf.set("userA", otherArgs[0]);
        conf.set("userB", otherArgs[1]);
        Job job = Job.getInstance(conf, "InMemoryJoin");
        job.setJarByClass(InMemoryJoin.class);
        job.setMapperClass(MFriend.Map.class);
        job.setReducerClass(MFriend.Reduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(otherArgs[2]));
        Path p = new Path(otherArgs[4] + "_temp");
        FileOutputFormat.setOutputPath(job, p);
        job.waitForCompletion(true);
        conf.set("userdata", otherArgs[3]);
        Job job2 = Job.getInstance(conf, "Sort");
        job2.setJarByClass(InMemoryJoin.class);
        job2.setInputFormatClass(KeyValueTextInputFormat.class);
        job2.setMapperClass(Map.class);
        job2.setReducerClass(Reduce.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job2, p);
        FileOutputFormat.setOutputPath(job2, new Path(otherArgs[4]));
        int code = job2.waitForCompletion(true) ? 0 : 1;
        FileSystem.get(conf).delete(p, true);
        System.exit(code);
    }
}