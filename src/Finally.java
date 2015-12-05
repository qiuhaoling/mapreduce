import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by qiuhaoling on 12/4/15.
 */
public class Finally {
    public static final class TAG_COUNT_MAPPER extends Mapper<LongWritable,Text,Text,Text> {

        public void map(LongWritable offset, Text value, Context context) throws IOException, InterruptedException {
            String input[] = value.toString().split("\t\t");
            context.write(new Text(input[0]),new Text(input[1]));
        }
    }
    public static final class TAG_COUNT_REDUCER extends Reducer<Text,Text,Text,IntWritable> {
        public void reduce(Text key, Iterable<Text> value, Context context) throws IOException, InterruptedException {
            int count = 0;
            for(Text it: value)
            {
                count += Integer.parseInt(it.toString());
            }
            if(count > 0)
                context.write(key,new IntWritable(count));
        }
    }
}
