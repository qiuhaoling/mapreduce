import java.io.IOException;
import java.net.URI;
import java.util.*;
import java.io.*;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.Job;

public class KMeans
{
    public static String CENTROID_FILE_NAME = "/centroid.txt";
    public static String OUTPUT_FILE_NAME = "/part-r-00000";
    public static String DATA_FILE_NAME = "/trainData.txt";
    public static String JOB_NAME = "KMeans";
    public static int K = 5;
    //public static String SPLITTER = "\t| ";
    //public static List<Double> mCenters = new ArrayList<Double>();
    /*
     * In Mapper class we are overriding configure function. In this we are
     * reading file from Distributed Cache and then storing that into instance
     * variable "mCenters"
     */
    public static class kMean_Map extends Mapper<LongWritable, Text, Text, Text> {

        public static List<Vector> mCenters;
        public static List<Boolean> mCentersMark;
        @Override
        public void setup(Context context) {
            mCenters = new ArrayList<Vector>();
            mCentersMark = new ArrayList<Boolean>();
            try {
                // Fetch the file from Distributed Cache Read it and store the
                // centroid in the ArrayList
                URI[] cacheFiles = context.getCacheFiles();
                if (cacheFiles != null && cacheFiles.length > 0) {
                    String line;
                    mCenters.clear();
                    BufferedReader cacheReader = new BufferedReader(
                            new FileReader(cacheFiles[0].toString()));
                    for(int i = 0;i<K;++i)
                    {
                        line = cacheReader.readLine();
                        mCenters.add(Vector.StringToVector(line));
                        mCentersMark.add(false);
                    }
                    cacheReader.close();
                }
            } catch (IOException e) {
                System.err.println("Exception reading DistribtuedCache: " + e);
            }
        }

        /*
         * Map function will find the minimum center of the point and emit it to
         * the reducer
         */
        @Override
        public void map(LongWritable key, Text value,
                        Context context) throws IOException, InterruptedException {
            //String line = value.toString();
            //double point = Double.parseDouble(line);
            String input = value.toString();
            Vector point = Vector.StringToVector(input);
            double min1, min2;
            min2= Double.MAX_VALUE;
            Vector nearest_center = mCenters.get(0);
            // Find the minimum center from a point
            int mark = 0;
            for (int i = 0;i<K;++i) {
                min1 = Vector.distance(mCenters.get(i),point);
                if (Math.abs(min1) < Math.abs(min2)) {
                    nearest_center = mCenters.get(i);
                    min2 = min1;
                    mark = i;
                }
            }
            mCentersMark.add(mark,true);
            context.write(new Text(nearest_center.VectorToString()),new Text(point.VectorToString()));
        }
        @Override
        public void cleanup(Context context) throws IOException, InterruptedException {
            for(int i = 0;i<K;++i)
            {
                if(mCentersMark.get(i) == false)
                {
                    context.write(new Text(mCenters.get(i).VectorToString()),new Text("NMSL"));
                }
            }
        }
    }

    public static class kMean_Data_Process extends Mapper<LongWritable, Text, Text, NullWritable> {

        public static List<Vector> mCenters;
        public static List<Boolean> mCentersMark;
        @Override
        public void setup(Context context) {
            mCenters = new ArrayList<Vector>();
            //mCentersMark = new ArrayList<Boolean>();
            try {
                // Fetch the file from Distributed Cache Read it and store the
                // centroid in the ArrayList
                URI[] cacheFiles = context.getCacheFiles();
                if (cacheFiles != null && cacheFiles.length > 0) {
                    String line;
                    mCenters.clear();
                    BufferedReader cacheReader = new BufferedReader(
                            new FileReader(cacheFiles[0].toString()));
                    for(int i = 0;i<K;++i)
                    {
                        line = cacheReader.readLine();
                        mCenters.add(Vector.StringToVector(line));
                        //mCentersMark.add(false);
                    }
                    cacheReader.close();
                }
            } catch (IOException e) {
                System.err.println("Exception reading DistribtuedCache: " + e);
            }
        }

        /*
         * Map function will find the minimum center of the point and emit it to
         * the reducer
         */
        @Override
        public void map(LongWritable key, Text value,
                        Context context) throws IOException, InterruptedException {
            //String line = value.toString();
            //double point = Double.parseDouble(line);
            String input = value.toString();
            Vector point = Vector.StringToVector(input);
            double min1, min2;
            min2= Double.MAX_VALUE;
            Vector nearest_center = mCenters.get(0);
            // Find the minimum center from a point
            int mark = 0;
            for (int i = 0;i<K;++i) {
                min1 = Vector.distance(mCenters.get(i),point);
                if (Math.abs(min1) < Math.abs(min2)) {
                    nearest_center = mCenters.get(i);
                    min2 = min1;
                    mark = i;
                }
            }
            //mCentersMark.add(mark,true);
            context.write(new Text(mark+","+point.VectorToString()),NullWritable.get());
        }
        /*
        @Override
        public void cleanup(Context context) throws IOException, InterruptedException {
            for(int i = 0;i<K;++i)
            {
                if(mCentersMark.get(i) == false)
                {
                    context.write(new Text(mCenters.get(i).VectorToString()),new Text("NMSL"));
                }
            }
        }
        */
    }

    public static class Reduce extends Reducer<Text, Text, Text, NullWritable> {

        /*
         * Reduce function will emit all the points to that center and calculate
         * the next center for these points
         */
        @Override
        public void reduce(Text key, Iterable<Text> value,
                           Context context) throws IOException, InterruptedException {
            Vector newCenter = new Vector();
            double sum[] = new double[Vector.Dimension];
            for(int i = 0;i<Vector.Dimension;++i)
                sum[i] = 0.0;
            int no_elements = 0;
            boolean mark = false;
            for(Text v:value)
            {
                Vector d;
                try{
                    d = Vector.StringToVector(v.toString());

                    for(int i = 0;i<Vector.Dimension;++i)
                    {
                        sum[i]+=d.vector[i];
                    }
                    ++no_elements;
                    mark = true;
                }
                catch(Exception e)
                {

                }
            }

            // We have new center now
            //newCenter = sum / no_elements;
            for(int i = 0;i<Vector.Dimension;++i)
                sum[i] = sum[i]/(double)no_elements;
            newCenter.vector = sum;
            // Emit new center and point
            if(mark == false)
            {
                context.write(key,NullWritable.get());
            }
            else
            {
                context.write(new Text(newCenter.VectorToString()), NullWritable.get());
            }
        }
    }

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



        Path randfile = new Path(CENTROID + CENTROID_FILE_NAME);
        FileSystem fs2 = FileSystem.get(new Configuration());
        BufferedWriter br2 = new BufferedWriter(new OutputStreamWriter(fs2.create(randfile)));
        Vector temp = new Vector();
        Random random = new Random();
        for(int j = 0;j<K;++j)
        {
            for(int i = 0;i<Vector.Dimension;++i) {
                temp.vector[i]=Math.abs(random.nextDouble()*3000);
            }
            br2.write(temp.VectorToString());
            br2.newLine();
        }
        br2.close();
        fs2.close();


        // Reiterating till the convergence
        int iteration = 0;
        boolean isdone = false;

        while (isdone == false) {
            Job job = new Job();
            //JobConf conf = new JobConf(KMeans.class);
            if (iteration == 0) {
                URI hdfsPath = new URI(CENTROID + CENTROID_FILE_NAME);
                // upload the file to hdfs. Overwrite any existing copy.
                //DistributedCache.addCacheFile(hdfsPath.toUri(), conf);
                job.addCacheFile(hdfsPath);
                ;
            } else {
                URI hdfsPath = new URI(again_input + OUTPUT_FILE_NAME);
                // upload the file to hdfs. Overwrite any existing copy.
                job.addCacheFile(hdfsPath);
            }
            //Configuration conf = getConf();
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
            List<Vector> centers_next = new ArrayList<Vector>();
            for(int i = 0;i<K;++i)
            {
                String l = br.readLine();
                if(l != null)
                    centers_next.add(Vector.StringToVector(l));
            }
            br.close();
            Collections.sort(centers_next);
            String prev;
            if (iteration == 0) {
                prev = CENTROID + CENTROID_FILE_NAME;
            } else {
                prev = again_input + OUTPUT_FILE_NAME;
            }
            URI prevfile = new URI(prev);
            BufferedReader br1 = new BufferedReader(new FileReader(prevfile.toString()));
            List<Vector> centers_prev = new ArrayList<Vector>();
            for(int i = 0;i<K;++i)
            {
                String l = br1.readLine();
                if(l != null)
                    centers_prev.add(Vector.StringToVector(l));
            }
            Collections.sort(centers_prev);
            br1.close();
            for(int i = 0;i<centers_next.size();++i)
            {
                double dist = Vector.distance(centers_next.get(i),centers_prev.get(i));
                if (dist <= 0.1) {
                    isdone = true;
                } else {
                    isdone = false;
                    break;
                }
            }
            ++iteration;
            again_input = output;
            output = OUT + System.nanoTime();
        }
        Job job = new Job();
        //JobConf conf = new JobConf(KMeans.class);
        URI hdfsPath = new URI(again_input + OUTPUT_FILE_NAME);
        // upload the file to hdfs. Overwrite any existing copy.
        job.addCacheFile(hdfsPath);
        //Configuration conf = getConf();
        job.setJobName(JOB_NAME);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        job.setMapperClass(kMean_Data_Process.class);
        //job.setReducerClass(Reduce.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.setInputPaths(job,
                new Path(input + DATA_FILE_NAME));
        FileOutputFormat.setOutputPath(job, new Path(output));

        job.waitForCompletion(true);
    }
}