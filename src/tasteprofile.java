import org.apache.accumulo.core.util.Pair;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by qiuhaoling on 11/29/15.
 */
public class tasteprofile {
    public static final class tasteprofileMapper extends Mapper<LongWritable,Text,Text,IntWritable>
    {

        public void map(LongWritable offset, Text value, Context context) throws IOException, InterruptedException {
            String inputStr[] = value.toString().split("\t");
            context.write(new Text(inputStr[1]), new IntWritable(Integer.parseInt(inputStr[2])));
        }
    }
    public static final class tasteprofileReducer extends Reducer<Text,IntWritable,Text,IntWritable>
    {
        public void reduce(Text key,Iterable<IntWritable> value,Context context) throws IOException, InterruptedException {
            int sum = 0;
            for(IntWritable it : value)
            {
                sum+=it.get();
            }
            context.write(key,new IntWritable(sum));
        }
    }

}