import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by qiuhaoling on 12/3/15.
 */
public class Point implements Writable {
    private double point[];
    Point(Point k)
    {
        this.point = new double[k.point.length];
        System.arraycopy(k.point,0,this.point,0,k.point.length);
    }
    @Override
    public void write(DataOutput dataOutput) throws IOException {

    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {

    }
}
