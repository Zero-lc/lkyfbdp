package ff;

import org.apache.hadoop.io.WritableComparable;
import java.io.DataInput;
import java.io.DataOutput;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.Logger;
import java.io.IOException;
import java.util.Arrays;


public class findfriend{
      
        public static class findfriendstep1Mapper extends Mapper<LongWritable, Text, Text, Text> {
            public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
                String line = value.toString(); 
                String[] userAndfriends = line.split(", ");      
                String user = userAndfriends[0];
                String[] friends = userAndfriends[1].split(" ");
                for (String friend : friends) {
                    context.write(new Text(friend), new Text(user)); 
                  
                }
            }
        }
    
        public static class findfriendstep1Reducer extends Reducer<Text, Text, Text, Text> {
            public void reduce(Text friend, Iterable<Text> users, Context context) throws IOException, InterruptedException {
                StringBuffer sb = new StringBuffer();
                for (Text user : users) {
                    sb.append(user).append(",");  
                }
                context.write(friend, new Text(sb.toString())); 
               
            }
        }
        public static class findfriendstep2Mapper extends Mapper<LongWritable, Text, Text, Text> {
            public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
                String line = value.toString();
                String[] friendAndusers = line.split("\t");
                String friend = friendAndusers[0];
                String[] users = friendAndusers[1].split(",");
                Arrays.sort(users);    
                for (int i = 0; i <= users.length - 2; i++) {    
                    for (int j = i + 1; j <= users.length - 1; j++) {
                        context.write(new Text("[" + users[i] + "," + users[j] + "]:"), new Text(friend));
                    }
                }
            }
        }
    
        public static class findfriendstep2Reducer extends Reducer<Text, Text, UserTestWritable, NullWritable> {
            private UserTestWritable out = new UserTestWritable();
            public void reduce(Text user, Iterable<Text> friends, Context context) throws IOException, InterruptedException {
                StringBuffer sb = new StringBuffer();
                for (Text friend : friends) {
                    sb.append(friend).append(","); 
                }
                out.setValue(sb);
                out.setKey(user);
                //String sbstr=sb.toString();
                //context.write(user, new Text("[" +(sbstr.substring(0,sbstr.length()-1))+"]"));
                
                context.write(out,NullWritable.get());
            }
        }
    
        public static void main(String[] args) throws Exception {
            Configuration conf = new Configuration();
            String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs(); 
            if (otherArgs.length < 2) {
                System.err.println("error");
                System.exit(2); 
            }
            Job job = Job.getInstance(conf, "find friends step1");
            job.setJarByClass(findfriend.class); 
            job.setMapperClass(findfriendstep1Mapper.class);
            job.setReducerClass(findfriendstep1Reducer.class);
            job.setOutputKeyClass(Text.class); 
            job.setOutputValueClass(Text.class);
            for (int i = 0; i < otherArgs.length - 2; ++i) {
                FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
            }
            FileOutputFormat.setOutputPath(job, new Path(otherArgs[otherArgs.length - 2]));
            if(job.waitForCompletion(true))
            {
                Job job2 = Job.getInstance(conf, "find friends step2");
                job2.setJarByClass(findfriend.class);
                job2.setMapperClass(findfriendstep2Mapper.class);
                job2.setReducerClass(findfriendstep2Reducer.class);
                job2.setMapOutputKeyClass(Text.class);
                job2.setMapOutputValueClass(Text.class);
                job2.setOutputKeyClass(UserTestWritable.class);
                job2.setOutputValueClass(NullWritable.class);
                FileInputFormat.addInputPath(job2, new Path(otherArgs[otherArgs.length - 2]));
                FileOutputFormat.setOutputPath(job2, new Path(otherArgs[otherArgs.length - 1]));
                System.exit(job2.waitForCompletion(true) ? 0 : 1);
            }
        }
}