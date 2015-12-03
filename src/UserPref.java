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
        Job job2=new Job(conf, "Tag-USER Join");
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

        FileInputFormat.setInputDirRecursive(job1, true);
        MultipleInputs.addInputPath(job1, new Path(args[0]), TextInputFormat.class, lastfm.class);
        MultipleInputs.addInputPath(job1, new Path(args[1]), TextInputFormat.class, unique_track.class);
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(Text.class);
        job1.setReducerClass(unique_track_join.class);

        FileOutputFormat.setOutputPath(job1, new Path(args[2]));

        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);
        job1.setOutputFormatClass(TextOutputFormat.class);

        job1.waitForCompletion(false);
        Log log = LogFactory.getLog(UserPref.class);
        log.info("Hello World!");



        job2.setJobName("Tag-USER Join");
        job2.setJarByClass(UserPref.class);

        FileInputFormat.setInputDirRecursive(job2, true);
        MultipleInputs.addInputPath(job2, new Path(args[2]), TextInputFormat.class, song_tag_mapper.class);
        MultipleInputs.addInputPath(job2, new Path(args[3]), TextInputFormat.class, tasteprofile.class);
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setReducerClass(GenreJoin.class);

        FileOutputFormat.setOutputPath(job2, new Path(args[4]));

        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);
        job2.setOutputFormatClass(TextOutputFormat.class);
        job2.waitForCompletion(false);

        return 0;
    }
    public static void main(String[] args) throws Exception {
        ToolRunner.run(new Configuration(), new UserPref(), args);
    }
}