import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.Random;

public class KMeans_NLINPUT {
    public static String DATA_FILE_NAME = "/trainData.txt";
    public static String JOB_NAME = "KMeans";

    public static void main(String[] args) throws Exception {
        run(args);
    }

    public static void run(String[] args) throws Exception {
        String IN = args[0];
        String ITER = args[1];
        String OUT = args[2];
        String input = IN;
        String output = OUT + System.nanoTime();
        String again_input = output;

        Job job = new Job();

        URI hdfsPath = new URI(IN + DATA_FILE_NAME);
        // upload the file to hdfs. Overwrite any existing copy.
        job.addCacheFile(hdfsPath);
        job.setJobName(JOB_NAME);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        job.setMapperClass(kMean_Map.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.setInputPaths(job,
                new Path(ITER));
        FileOutputFormat.setOutputPath(job, new Path(output));

        job.waitForCompletion(false);
    }

    /*
     * In Mapper class we are overriding configure function. In this we are
     * reading file from Distributed Cache and then storing that into instance
     * variable "mCenters"
     */
    public static class kMean_Map extends Mapper<LongWritable, Text, Text, NullWritable> {

        public static URI[] cacheFiles;

        @Override
        public void setup(Context context) throws IOException {
            cacheFiles = context.getCacheFiles();
        }

        /*
         * Map function will find the minimum center of the point and emit it to
         * the reducer
         */
        @Override
        public void map(LongWritable key, Text value,
                        Context context) throws IOException, InterruptedException {
            Boolean isdone = false;
            Vector[] mCenters = null;
            Vector[] newCenters = null;
            int[] mCentersMark = null;
            int K = Integer.parseInt(value.toString());
            mCentersMark = new int[K];
            int iterator = 1;
            BufferedReader br = null;
            while (isdone == false) {
                Log log = LogFactory.getLog(KMeans_NLINPUT.class);
                log.info("We have K=" + K + " ,this is Iter" + iterator);
                br = new BufferedReader(new FileReader(cacheFiles[0].toString()));
                Vector temp = null;
                Vector temp2 = null;
                Random random = new Random();
                if (iterator == 1) {
                    mCenters = new Vector[K];
                    for (int j = 0; j < K; ++j) {
                        temp = new Vector();
                        for (int i = 0; i < Vector.Dimension; ++i) {
                            temp.vector[i] = Math.abs(random.nextDouble() * 3000);
                        }
                        mCenters[j] = temp;

                    }
                }
                newCenters = new Vector[K];
                for (int j = 0; j < K; ++j) {
                    temp2 = new Vector();
                    for (int i = 0; i < Vector.Dimension; ++i) {
                        temp2.vector[i] = 0;
                    }
                    newCenters[j] = temp2;
                    mCentersMark[j] = 0;
                }
                String str = null;
                while ((str = br.readLine()) != null) {
                    String input = str;
                    Vector point = Vector.InputParser(input);
                    double min1, min2;
                    min2 = Double.MAX_VALUE;
                    // Find the minimum center from a point
                    int mark = 0;
                    for (int i = 0; i < K; ++i) {
                        min1 = Vector.distance(mCenters[i], point);
                        if (Math.abs(min1) < Math.abs(min2)) {
                            min2 = min1;
                            mark = i;
                        }
                    }
                    mCentersMark[mark]++;
                    for (int i = 0; i < Vector.Dimension; ++i) {
                        newCenters[mark].vector[i] += point.vector[i];
                    }

                }
                br.close();
                for (int i = 0; i < K; ++i) {
                    if (mCentersMark[i] == 0) {
                        newCenters[i] = mCenters[i];
                    } else {
                        for (int j = 0; j < Vector.Dimension; ++j) {
                            newCenters[i].vector[j] /= (double) mCentersMark[i];
                        }
                    }
                }
                java.util.Arrays.sort(mCenters);
                java.util.Arrays.sort(newCenters);
                for (int i = 0; i < K; ++i) {
                    double dist = Vector.distance(mCenters[i], newCenters[i]);
                    if (dist < 0.1) {
                        isdone = true;
                    } else {
                        isdone = false;
                        break;
                    }
                }
                mCenters = newCenters;
                log.info("Iter " + iterator + " finished!");
                iterator++;
            }
            for (int i = 0; i < K; ++i)
                context.write(new Text(newCenters[i].VectorToString()), NullWritable.get());


        }

        @Override
        public void cleanup(Context context) throws IOException, InterruptedException {

        }
    }
}