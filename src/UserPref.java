/**
 * Created by qiuhaoling on 11/28/15.
 */

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FilterFileSystem;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class UserPref extends Configured implements Tool {
    public int run(String[] args) throws Exception {
        //Job job = new Job(new Configuration(), "UserPref");
        Configuration conf = getConf();
        FileSystem fs = FileSystem.get(conf);
        Job job = new Job(conf, "Job1");
        Job job1=new Job(conf, "Tag-SID Join");
        Job job2=new Job(conf, "User-COUNT Join");
        /*
        FileSystem fs = new FilterFileSystem();
        try{
            fs.delete(new Path(args[2]), true);
        }
        catch(Exception e)
        {

        }
        try{
            fs.delete(new Path(args[4]), true);
        }
        catch(Exception e)
        {

        }
        */
        job1.setJobName("Tag-SID Join");
        job1.setJarByClass(UserPref.class);

        MultipleInputs.addInputPath(job1, new Path(args[0]), TextInputFormat.class, lastfm.class);
        MultipleInputs.addInputPath(job1, new Path(args[1]), TextInputFormat.class, unique_track.class);
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(Text.class);
        job1.setReducerClass(unique_track_join.class);
        String intermediate1 = args[3]+System.nanoTime();
        FileOutputFormat.setOutputPath(job1, new Path(intermediate1));

        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);
        job1.setOutputFormatClass(TextOutputFormat.class);

        job1.waitForCompletion(false);
        Log log = LogFactory.getLog(UserPref.class);
        log.info("Hello World!");



        job2.setJobName("User-Count Join");
        job2.setJarByClass(UserPref.class);

        MultipleInputs.addInputPath(job2, new Path(args[2]), TextInputFormat.class, tasteprofile.tasteprofileMapper.class);
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(IntWritable.class);
        job2.setReducerClass(tasteprofile.tasteprofileReducer.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(IntWritable.class);
        job2.setOutputFormatClass(TextOutputFormat.class);
        String intermediate2 = args[3]+System.nanoTime();
        FileOutputFormat.setOutputPath(job2, new Path(intermediate2));
        job2.waitForCompletion(false);

        log = LogFactory.getLog(UserPref.class);
        log.info("Hello World!");


        Job job3 = new Job();
        job3.setJobName("Tag-Count");
        job3.setJarByClass(UserPref.class);

        MultipleInputs.addInputPath(job3, new Path(intermediate1), TextInputFormat.class, Tag_Count.SID_TAG_MAPPER.class);
        MultipleInputs.addInputPath(job3, new Path(intermediate2), TextInputFormat.class, Tag_Count.SID_COUNT_MAPPER.class);
        job3.setMapOutputKeyClass(Text.class);
        job3.setMapOutputValueClass(Text.class);
        job3.setReducerClass(Tag_Count.TAG_COUNT.class);
        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(IntWritable.class);
        job3.setOutputFormatClass(TextOutputFormat.class);
        String intermediate3 = args[3]+System.nanoTime();
        FileOutputFormat.setOutputPath(job3, new Path(intermediate3));
        job3.waitForCompletion(false);

        log = LogFactory.getLog(UserPref.class);
        log.info("Hello World!");


        Job job4 = new Job();
        job4.setJobName("User-Count Join");
        job4.setJarByClass(UserPref.class);

        MultipleInputs.addInputPath(job4, new Path(intermediate3), TextInputFormat.class, Finally.TAG_COUNT_MAPPER.class);
        job4.setMapOutputKeyClass(Text.class);
        job4.setMapOutputValueClass(Text.class);
        job4.setReducerClass(Finally.TAG_COUNT_REDUCER.class);
        job4.setOutputKeyClass(Text.class);
        job4.setOutputValueClass(IntWritable.class);
        job4.setOutputFormatClass(TextOutputFormat.class);
        FileOutputFormat.setOutputPath(job4, new Path(args[3]+System.nanoTime()));
        job4.waitForCompletion(false);

        return 0;
    }
    public static void main(String[] args) throws Exception {
        ToolRunner.run(new Configuration(), new UserPref(), args);
    }
}