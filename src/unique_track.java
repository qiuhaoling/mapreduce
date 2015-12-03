import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;

/**
 * Created by qiuhaoling on 12/2/15.
 */
public class unique_track extends Mapper<LongWritable,Text,Text,Text>{

        public void map(LongWritable offset, Text value, Mapper.Context context) throws IOException, InterruptedException {
            String Str[] = value.toString().split("<SEP>");
            context.write(new Text(Str[0]),new Text(Str[1]));
        }

}
