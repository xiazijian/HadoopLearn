package cn.xwy.hadoop;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class OnlyMap {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf,"OnlyMap");
        job.setJarByClass(OnlyMap.class);
        job.setMapperClass(OnlyMap.Map.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        Path inputpath = new Path("D:\\Codes\\IDEA\\hadoopLearn\\join_data\\table1.csv");
        //Path inputpath = new Path(args[0]);
        FileInputFormat.addInputPath(job,inputpath);
        Path outputpath = new Path("D:\\Codes\\IDEA\\hadoopLearn\\onlymap_output");
        //Path outputpath = new Path(args[1]);
        FileOutputFormat.setOutputPath(job,outputpath);
        System.exit(job.waitForCompletion(true)?0:1);
    }
    public static class Map extends Mapper<Object, Text,Text,Text> {
        public void map(Object key,Text value,Context context) throws IOException,InterruptedException{
            context.write(new Text(String.valueOf(key)),value);
        }
    }
}
