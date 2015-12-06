import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.*;
import java.net.URI;
import java.util.Random;

public class KMeans {
    public static String CENTROID_FILE_NAME = "/centroid.txt";
    public static String OUTPUT_FILE_NAME = "/part-r-00000";
    public static String DATA_FILE_NAME = "/song_tag_count.csv";
    public static String JOB_NAME = "KMeans";
    public static int K = 5;

    public static void main(String[] args) throws Exception {
        run(args);
    }

    public static void run(String[] args) throws Exception {
        String IN = args[0];
        String OUT = args[1];
        String CENTROID = args[2];
        String input = IN;
        String output = OUT + System.nanoTime();
        String again_input = output;

        URI randfile = new URI(CENTROID + CENTROID_FILE_NAME);
        FileSystem fs2 = FileSystem.get(new Configuration());
        BufferedWriter br2 = new BufferedWriter(new FileWriter(randfile.toString()));
        Vector temp;
        Random random = new Random();
        for (int j = 0; j < K; ++j) {
            temp = new Vector();
            for (int i = 0; i < Vector.Dimension; ++i) {
                temp.vector[i] = Math.abs(random.nextDouble() * 3000);
            }
            br2.write(temp.VectorToString());
            br2.newLine();
        }
        br2.close();
        fs2.close();

        // Reiterating till the convergence
        int iterator = 1;
        boolean isdone = false;

        while (isdone == false) {
            Job job = new Job();
            Log log = LogFactory.getLog(KMeans.class);
            log.info("We have K="+K+" ,this is Iter"+ iterator);
            if (iterator == 1) {
                URI hdfsPath = new URI(CENTROID + CENTROID_FILE_NAME);
                // upload the file to hdfs. Overwrite any existing copy.
                job.addCacheFile(hdfsPath);
                ;
            } else {
                URI hdfsPath = new URI(again_input + OUTPUT_FILE_NAME);
                // upload the file to hdfs. Overwrite any existing copy.
                job.addCacheFile(hdfsPath);
            }
            job.setJobName(JOB_NAME);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(NullWritable.class);
            job.setMapperClass(kMean_Map.class);
            job.setReducerClass(Reduce.class);
            job.setInputFormatClass(TextInputFormat.class);
            job.setOutputFormatClass(TextOutputFormat.class);

            FileInputFormat.setInputPaths(job,
                    new Path(input + DATA_FILE_NAME));
            FileOutputFormat.setOutputPath(job, new Path(output));

            job.waitForCompletion(true);


            URI ofile = new URI(output + OUTPUT_FILE_NAME);
            BufferedReader br = new BufferedReader(new FileReader(ofile.toString()));
            //List<Vector> centers_next = new ArrayList<Vector>();
            Vector centers_next[] = new Vector[K];
            for (int i = 0; i < K; ++i) {
                String l = br.readLine();
                centers_next[i] = Vector.StringToVector(l);
            }
            br.close();
            java.util.Arrays.sort(centers_next);

            String prev;
            if (iterator == 1) {
                prev = CENTROID + CENTROID_FILE_NAME;
            } else {
                prev = again_input + OUTPUT_FILE_NAME;
            }

            URI prevfile = new URI(prev);
            br = new BufferedReader(new FileReader(prevfile.toString()));
            //List<Vector> centers_prev = new ArrayList<Vector>();
            Vector centers_prev[] = new Vector[K];
            for (int i = 0; i < K; ++i) {
                String l = br.readLine();
                centers_prev[i] = Vector.StringToVector(l);
            }
            java.util.Arrays.sort(centers_prev);
            br.close();
            for (int i = 0; i < K; ++i) {
                double dist = Vector.distance(centers_next[i], centers_prev[i]);
                if (dist <= 0.1) {
                    isdone = true;
                } else {
                    isdone = false;
                    break;
                }
            }

            log.info("Iter "+iterator+" finished!");
            ++iterator;
            again_input = output;
            output = OUT + System.nanoTime();
        }
/*
        Job job = new Job();
        URI hdfsPath = new URI(again_input + OUTPUT_FILE_NAME);
        // upload the file to hdfs. Overwrite any existing copy.
        job.addCacheFile(hdfsPath);
        job.setJobName(JOB_NAME);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        job.setMapperClass(kMean_Data_Process.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.setInputPaths(job,
                new Path(input + DATA_FILE_NAME));
        FileOutputFormat.setOutputPath(job, new Path(output));

        job.waitForCompletion(false);
*/
    }

    /*
     * In Mapper class we are overriding configure function. In this we are
     * reading file from Distributed Cache and then storing that into instance
     * variable "mCenters"
     */
    public static class kMean_Map extends Mapper<LongWritable, Text, Text, Text> {

        public static Vector mCenters[];
        public static Boolean mCentersMark[];

        @Override
        public void setup(Context context) throws IOException {
            mCenters = new Vector[K];
            mCentersMark = new Boolean[K];
            // Fetch the file from Distributed Cache Read it and store the
            // centroid in the ArrayList
            URI[] cacheFiles = context.getCacheFiles();
            String line;
            BufferedReader cacheReader = new BufferedReader(
                    new FileReader(cacheFiles[0].toString()));
            for (int i = 0; i < K; ++i) {
                line = cacheReader.readLine();
                mCenters[i] = Vector.StringToVector(line);
                mCentersMark[i] = false;
            }
            cacheReader.close();
        }

        /*
         * Map function will find the minimum center of the point and emit it to
         * the reducer
         */
        @Override
        public void map(LongWritable key, Text value,
                        Context context) throws IOException, InterruptedException {
            String input = value.toString();
            Vector point = Vector.InputParser(input);
            double min1, min2;
            min2 = Double.MAX_VALUE;
            Vector nearest_center = null;
            // Find the minimum center from a point
            int mark = 0;
            for (int i = 0; i < K; ++i) {
                min1 = Vector.distance(mCenters[i], point);
                if (Math.abs(min1) < Math.abs(min2)) {
                    nearest_center = mCenters[i];
                    min2 = min1;
                    mark = i;
                }
            }
            mCentersMark[mark] = true;
            context.write(new Text(nearest_center.VectorToString()), new Text(point.VectorToString()));
        }

        @Override
        public void cleanup(Context context) throws IOException, InterruptedException {
            for (int i = 0; i < K; ++i) {
                if (mCentersMark[i] == false) {
                    context.write(new Text(mCenters[i].VectorToString()), new Text("NMSL"));
                }
            }
        }
    }
/*
    public static class kMean_Data_Process extends Mapper<LongWritable, Text, Text, NullWritable> {

        public static Vector mCenters[];
        public static Boolean mCentersMark[];

        @Override
        public void setup(Context context) throws IOException {
            mCenters = new Vector[K];
            mCentersMark = new Boolean[K];
            // Fetch the file from Distributed Cache Read it and store the
            // centroid in the ArrayList
            URI[] cacheFiles = context.getCacheFiles();
            String line;
            BufferedReader cacheReader = new BufferedReader(
                    new FileReader(cacheFiles[0].toString()));
            for (int i = 0; i < K; ++i) {
                line = cacheReader.readLine();
                mCenters[i] = Vector.StringToVector(line);
                mCentersMark[i] = false;
            }
            cacheReader.close();
        }

        @Override
        public void map(LongWritable key, Text value,
                        Context context) throws IOException, InterruptedException {
            String input = value.toString();
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
            context.write(new Text(mark + "," + point.VectorToString()), NullWritable.get());
        }
    }
*/
    public static class Reduce extends Reducer<Text, Text, Text, NullWritable> {

        /*
         * Reduce function will emit all the points to that center and calculate
         * the next center for these points
         */
        @Override
        public void reduce(Text key, Iterable<Text> value,
                           Context context) throws IOException, InterruptedException {
            Vector newCenter = new Vector();
            for (int i = 0; i < Vector.Dimension; ++i)
                newCenter.vector[i] = 0.0;
            int no_elements = 0;
            boolean mark = false;
            for (Text v : value) {
                Vector d;
                try {
                    d = Vector.StringToVector(v.toString());

                    for (int i = 0; i < Vector.Dimension; ++i) {
                        newCenter.vector[i] += d.vector[i];
                    }
                    ++no_elements;
                    mark = true;
                } catch (Exception e) {

                }
            }

            // Emit new center and point
            if (mark == false) {
                context.write(key, NullWritable.get());
            } else {
                for (int i = 0; i < Vector.Dimension; ++i)
                    newCenter.vector[i] = newCenter.vector[i] / (double) no_elements;
                context.write(new Text(newCenter.VectorToString()), NullWritable.get());
            }
        }
    }
}