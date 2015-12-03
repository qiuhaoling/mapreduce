/**
 * Created by qiuhaoling on 12/2/15.
 */
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
public class unique_track_join extends Reducer<Text,Text,Text,Text> {
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Text outputKey = new Text();
        Text outputValue = new Text();
        while(values.iterator().hasNext())
        {
            String tmp = values.iterator().next().toString();
            if(tmp.length() == 18 && tmp.charAt(0) == 'S')
            {
                outputKey.set(tmp);
            }
            else
            {
                outputValue.set(tmp);
            }
        }
        if(outputKey.getLength()>0 && outputValue.getLength()>0)
            context.write(new Text(outputKey.toString() +","+outputValue.toString()),new Text(""));
    }
}
