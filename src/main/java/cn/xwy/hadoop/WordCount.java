package cn.xwy.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.StringTokenizer;
public class WordCount {

    public static class TokenizerMapper extends Mapper<Object, Text,Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(Object key,Text value,Context context)
                throws IOException,InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()){
                word.set(itr.nextToken());
                context.write(word,one);
            }
        }
    }
    public static class IntSumReader extends Reducer<Text,IntWritable,Text,IntWritable> {
        private IntWritable result = new IntWritable();
        public void reduce (Text key,Iterable<IntWritable> values,Context context)
                throws IOException,InterruptedException{
            int sum = 0;
            for (IntWritable val:values){
                sum += val.get();
            }
            result.set(sum);
            context.write(key,result);
        }
    }
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf,"word count");
        job.setJarByClass(WordCount.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReader.class);
        job.setReducerClass(IntSumReader.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        //Path inputpath = new Path("D:\\Codes\\IDEA\\hadoopLearn\\input_data.txt");
        Path inputpath = new Path(args[0]);
        FileInputFormat.addInputPath(job,inputpath);
        //Path outputpath = new Path("D:\\Codes\\IDEA\\hadoopLearn\\output_data.txt");
        Path outputpath = new Path(args[1]);
        FileOutputFormat.setOutputPath(job,outputpath);
        System.exit(job.waitForCompletion(true)?0:1);
    }
}
