import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by qiuhaoling on 12/4/15.
 */
public class Tag_Count {
    public static final class SID_TAG_MAPPER extends Mapper<LongWritable,Text,Text,Text> {

        public void map(LongWritable offset, Text value, Context context) throws IOException, InterruptedException {
            String input[] = value.toString().split(",");
            context.write(new Text(input[0]),new Text(input[1]));
        }
    }
    public static final class SID_COUNT_MAPPER extends Mapper<LongWritable,Text,Text,Text> {

        public void map(LongWritable offset, Text value, Context context) throws IOException, InterruptedException {
            String input[] = value.toString().split("\t");
            context.write(new Text(input[0]),new Text(input[1]));
        }
    }
    public static final class TAG_COUNT extends Reducer<Text,Text,Text,IntWritable> {
        public void reduce(Text key, Iterable<Text> value, Context context) throws IOException, InterruptedException {
            int count = 0;
            String tmp,tmp2=null;
            for(Text it : value)
            {
                tmp = it.toString();
                try
                {
                    count = Integer.parseInt(tmp);
                }
                catch(Exception e)
                {
                    tmp2 = new String(tmp);
                }
            }
            if(count > 0 && tmp2 != null)
                context.write(new Text(tmp2),new IntWritable(count));
        }
    }
}
