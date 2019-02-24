import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;

public class InMemoryJoin extends Configured implements Tool {

    public static class Map extends Mapper<LongWritable, Text, Text, Text> {
        private Text list = new Text();
        private Text userid = new Text();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            Configuration config = context.getConfiguration();
            HashMap<String, String> userData = new HashMap<>();
            String userDataPath = config.get("userdata");
            FileSystem fs = FileSystem.get(config);
            Path path = new Path(userDataPath);
            BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path)));
            String line = br.readLine();
            while (line != null) {
                String[] arr = line.split(",");
                if (arr.length == 10) {
                    String data = arr[1] + ":" + arr[9];
                    userData.put(arr[0].trim(), data);
                }
                line = br.readLine();
            }

            String[] user = value.toString().split("\t");
            if ((user.length == 2)) {

                String[] friend_list = user[1].split(",");
                int n = friend_list.length;
                StringBuilder res = new StringBuilder("[");
                for (int i = 0; i < n; i++) {
                    if (userData.containsKey(friend_list[i])) {
                        if (i == (n - 1))
                            res.append(userData.get(friend_list[i]));
                        else {
                            res.append(userData.get(friend_list[i]));
                            res.append(",");
                        }
                    }
                }
                res.append("]");
                userid.set(user[0]);
                list.set(res.toString());
                context.write(userid, list);
            }

        }
    }

    // Driver program
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new InMemoryJoin(), args);
        System.exit(res);

    }

    @Override
    public int run(String[] otherArgs) throws Exception {
        // TODO Auto-generated method stub
        Configuration conf = new Configuration();
        if (otherArgs.length != 6) {
            System.err.println("Usage: InMemoryJoin <userA> <userB> <in> <out> <userdata> <userout>");
            System.exit(2);
        }
        conf.set("userA", otherArgs[0]);
        conf.set("userB", otherArgs[1]);
        Job job = Job.getInstance(conf, "InlineArgument");
        job.setJarByClass(InMemoryJoin.class);
        job.setMapperClass(MutualFriends.Map.class);
        job.setReducerClass(MutualFriends.Reduce.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(otherArgs[2]));
        Path p = new Path(otherArgs[3]);
        FileOutputFormat.setOutputPath(job, p);
        int code;

        Configuration conf1 = getConf();
        conf1.set("userdata", otherArgs[4]);
        Job job2 = Job.getInstance(conf1, "InMemoryJoin");
        job2.setJarByClass(InMemoryJoin.class);

        job2.setMapperClass(Map.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job2, p);
        FileOutputFormat.setOutputPath(job2, new Path(otherArgs[5]));

        code = job2.waitForCompletion(true) ? 0 : 1;
        System.exit(code);
        return code;

    }

}
