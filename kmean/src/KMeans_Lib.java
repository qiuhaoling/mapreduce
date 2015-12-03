import java.util.ArrayList;
import java.util.Random;

/**
 * Created by qiuhaoling on 11/30/15.
 */
public class KMeans_Lib {
    int k;
    int dataNum;
    int dimNum;
    ArrayList Tuple;
    double getDistXY(ArrayList<Double> t1,ArrayList<Double> t2)
    {
        double sum = 0;
        for(int i = 1; i < this.dimNum;++i)
        {
            sum += (t1.get(i)-t2.get(i)) * (t1.get(i)-t2.get(i));
        }
        return Math.sqrt(sum);
    }
    int clusterOfTuple(ArrayList<Double> means[],ArrayList<Double> tuple)
    {
        double dist = getDistXY(means[0],this.Tuple);
        double tmp;
        int label = 0;
        for(int i = 1;i<this.k;i++)
        {
            tmp = getDistXY(means[i],this.Tuple);
            if(tmp<dist)
            {
                dist = tmp;
                label = i;
            }
        }
        return label;
    }
    double getVar(ArrayList<ArrayList<Double>> clusters[],ArrayList<Double> means[])
    {
        double var = 0;
        for(int i = 0 ; i < this.k ; i++)
        {
            ArrayList<ArrayList<Double>> t = clusters[i];
            for(int j  = 0; j< t.size();j++)
            {
                var += getDistXY(t.get(j),means[i]);
            }
        }
        return var;
    }
    ArrayList<Double> getMeans(ArrayList<ArrayList<double>> cluster)
    {
        int num = cluster.size();
        ArrayList<Double> t = new ArrayList<Double>(this.dimNum+1);
        for(double it : t)
        {
            it = 0.0;
        }
        t[2] = 0.0;
        for(int i = 0;i<num;i++)
        {
            for(int j = 1;j<=this.dimNum;++j)
            {
                t.add(j,t.get(j)+cluster.get(i).get(j));
            }
        }
        for(int j = 1;j<this.dimNum;++j)
        {
            t.add(j,t.get(j)/num);
        }
        return t;
    }
    void print(ArrayList<ArrayList<Double>> clusters[])
    {
        for(int label = 0;label < k;label++)
        {
            System.out.println("第第No."+ label+1 + "Cluster");
            ArrayList<ArrayList<Double>> t = clusters[label];
            for(int i = 0;i<t.size();i++)
            {
                System.out.print(".(");
                for(int j = 0;j<=this.dimNum;++j)
                {
                    System.out.print(t.get(i).get(j)+",");
                }
                System.out.print(")\n");
            }
        }
    }
    void KMeans(ArrayList<ArrayList<Double>> tuples)
    {
        ArrayList<ArrayList<Double>> clusters[] = new ArrayList[this.k];
        ArrayList<Double> means[] = new ArrayList[this.k];
        int i = 0;
        Random random = new Random();
        for(i = 0;i<k;)
        {
            int iToSelect = random.nextInt()%tuples.size();
            if(means[iToSelect].size() == 0)
            {
                for(int j = 0;j<=dimNum;++j)
                {
                    means[i].add(tuples.get(iToSelect).get(j));
                }
                i++;
            }
        }
        int label = 0;
        for(i = 0;i!=tuples.size();++i)
        {
            label = clusterOfTuple(means,tuples.get(i));
            clusters[label].add(tuples[i]);
        }
        double oldVar = -1;
        double newVar = getVar(clusters,means);
        int t = 0;
        while(Math.abs(newVar-oldVar)>=1)
        {
            for(i=0;i<k;++i)
            {
                clusters[i] = getMeans(clusters[i]);
            }
            oldVar = newVar;
            newVar = getVar(clusters,means);
            for(i = 0;i<k;++i)
            {
                clusters[i].clear();
            }
            for(i = 0;i!=tuples.size();++i)
            {
                label = clusterOfTuple(means,tuples.get(i));
                clusters[label].add(tuples.get(i));
            }
        }
    }
}
