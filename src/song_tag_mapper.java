import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by qiuhaoling on 12/2/15.
 */
public class song_tag_mapper extends Mapper<LongWritable,Text,Text,Text> {
    public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException {
        String Str[] = value.toString().split(",");
        context.write(new Text(Str[0]),new Text(Str[1]));
    }
}
