import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.StringUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashSet;
import java.util.Objects;

class MFriend {
    public static class Map extends Mapper<LongWritable, Text, Text, Text> {
        Long temp = -1L;
        Long fUser = -1L;
        Long sUser = -1L;
        Long inputUsr = -1L;
        String u1 = "", u2 = "";

        public void setup(Context context) {
            Configuration config = context.getConfiguration();
            u1 = config.get("userA");
            u2 = config.get("userB");
            fUser = Long.parseLong(u1);
            sUser = Long.parseLong(u2);
        }

        private Text m_others = new Text();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] split = line.split("\t");
            String subject = split[0];
            inputUsr = Long.parseLong(subject);
            if (split.length == 2) {
                String others = split[1];
                if ((inputUsr.equals(fUser)) || (inputUsr.equals(sUser))) {
                    m_others.set(others);
                    if (inputUsr.equals(fUser))
                        temp = sUser;
                    else
                        temp = fUser;
                    UserPageWritable data;
                    String sol;
                    if (inputUsr.compareTo(temp) < 0) {
                        data = new UserPageWritable(inputUsr, temp);
                        sol = data.toString();
                        context.write(new Text(sol), m_others);
                    } else {
                        data = new UserPageWritable(temp, inputUsr);
                        sol = data.toString();
                        context.write(new Text(sol), m_others);
                    }
                }
            }
        }
    }

    public static class Reduce extends Reducer<Text, Text, Text, Text> {

        private HashSet<Integer> intersection(String s1, String s2) {
            HashSet<Integer> hash1 = new HashSet<>();
            HashSet<Integer> hash2 = new HashSet<>();
            if (null != s1) {
                String[] s = s1.split(",");
                for (String s3 : s) {
                    hash1.add(Integer.parseInt(s3));
                }
            }
            if (null != s2) {
                String[] sa = s2.split(",");
                for (String s : sa) {
                    if (hash1.contains(Integer.parseInt(s))) {
                        hash2.add(Integer.parseInt(s));
                    }
                }
            }
            return hash2;
        }

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String[] combined = new String[2];
            int cur = 0;
            for (Text value : values) {
                combined[cur++] = value.toString();
            }
            if (null != combined[0]) {
                combined[0] = combined[0].replaceAll("[^0-9,]", "");
            }
            if (null != combined[1]) {
                combined[1] = combined[1].replaceAll("[^0-9,]", "");
            }
            HashSet<Integer> ca = intersection(combined[0], combined[1]);
            context.write(new Text(key.toString()), new Text(StringUtils.join(",", ca)));
        }
    }

    public static class UserPageWritable implements WritableComparable<UserPageWritable> {
        private Long userId, friendId;

        UserPageWritable(Long user, Long friend1) {
            this.userId = user;
            this.friendId = friend1;
        }

        public void readFields(DataInput in) throws IOException {
            userId = in.readLong();
            friendId = in.readLong();
        }

        public void write(DataOutput out) throws IOException {
            out.writeLong(userId);
            out.writeLong(friendId);
        }

        public int compareTo(UserPageWritable o) {
            int result = userId.compareTo(o.userId);
            if (result != 0)
                return result;
            return this.friendId.compareTo(o.friendId);
        }

        public String toString() {
            return userId.toString() + "  " + friendId.toString();
        }

        public boolean equals(Object obj) {
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            final UserPageWritable other = (UserPageWritable) obj;
            if (!Objects.equals(this.userId, other.userId)) {
                return false;
            }
            return Objects.equals(this.friendId, other.friendId);
        }

    }
}