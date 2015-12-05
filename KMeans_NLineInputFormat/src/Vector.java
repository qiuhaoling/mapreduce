import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by qiuhaoling on 12/3/15.
 */
public class Vector implements WritableComparable<Vector> {
    public static int Dimension = 2;
    public static int offset = 0;
    public double vector[];

    public Vector() {
        super();
        vector = new double[this.Dimension];
    }

    public static double distance(Vector src, Vector dst) {
        //if(src.vector.size()!=dst.vector.size())return -1;
        double sum = 0;
        for (int i = 0; i < Dimension; ++i) {
            sum += Math.pow(src.vector[i] - dst.vector[i], (double) Dimension);
        }
        return Math.pow(sum, 1 / (double) Dimension);
    }

    public static Vector StringToVector(String in) {
        String str[] = in.split(",");
        Vector result = new Vector();
        for (int i = 0; i < str.length; ++i) {
            result.vector[i] = Double.parseDouble(str[i]);
        }
        return result;
    }

    private static double Hashfun(String str) {
        //byte[] data = str.getBytes();
        //return (double) SipHashInline.hash24(0, 0, data);
        return Double.parseDouble(str);
    }

    public static Vector InputParser(String str) {
        String input[] = str.split(",");
        Vector result = new Vector();
        for (int i = 0; i < Dimension; ++i) {
            result.vector[i] = Hashfun(input[i + offset]);
        }
        return result;
    }

    @Override
    public int compareTo(Vector o) {
        return (int) (this.vector[0] - o.vector[0]);

    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        for (double it : vector)
            dataOutput.writeDouble(it);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        for (int i = 0; i < Dimension; ++i) {
            double tmp = dataInput.readDouble();
            this.vector[i] = tmp;
        }
    }

    public String VectorToString() {
        String result = new String();
        for (int i = 0; i < Dimension; ++i) {
            result += this.vector[i] + ",";
        }
        result = result.substring(0, result.length() - 1);
        return result;
    }

}