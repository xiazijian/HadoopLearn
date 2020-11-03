package cn.xwy.hadoop.TotalOrderPartitioner;

import cn.xwy.hadoop.customize_partition.MonthDoWWritable;
import cn.xwy.hadoop.customize_partition.SortAscMonthDescWeekMRJob;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.net.URI;

public class SortUsingTotalOrderPartitioner extends Configured implements Tool {
    public static void main(String[]args) throws Exception {
        String arg = "-D mapreduce.job.reduces=10 ./input/totalOrderPartitioner ./output/customize_partition partition.lst";
        String[] Args = arg.split(" ");
        ToolRunner.run(new SortUsingTotalOrderPartitioner(), Args);
    }
    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        conf.set("mapreduce.totalorderpartitioner.naturalorder","false");
        Job job = Job.getInstance(conf,"SortUsingTotalOrderPartitioner");
        job.setJarByClass(SortUsingTotalOrderPartitioner.class);
        job.setInputFormatClass(KeyValueTextInputFormat.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        /*job.setMapOutputKeyClass(MonthDoWWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(MonthDoWWritable.class);
        job.setOutputValueClass(Text.class);
        job.setMapperClass(SortMapper.class);
        job.setReducerClass(SortReducer.class);*/
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        // partitioner class设置成TotalOrderPartitioner
        job.setPartitionerClass(TotalOrderPartitioner.class);

        // RandomSampler第一个参数表示key会被选中的概率，第二个参数是一个选取samples数，第三个参数是最大读取input splits数
        //InputSampler.SplitSampler<Text, Text> sampler = new InputSampler.SplitSampler<Text, Text>(3, 10);
        InputSampler.RandomSampler<Text, Text> sampler = new InputSampler.RandomSampler<Text, Text>(0.9, 1, 1);
        // 写partition file到mapreduce.totalorderpartitioner.path
        InputSampler.writePartitionFile(job, sampler);
        // 设置partition file全路径到conf
        TotalOrderPartitioner.setPartitionFile(conf, new Path(args[2]));
        /*String partitionFile = TotalOrderPartitioner.getPartitionFile(job.getConfiguration());
        URI uri = new URI(partitionFile+"#"+TotalOrderPartitioner.DEFAULT_PATH);
        job.addCacheFile(uri);*/



        job.waitForCompletion(true);
        return 0;
    }
    /*public static class SortMapper extends
            Mapper<LongWritable, Text, MonthDoWWritable, Text> {
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] s = StringUtils.split(value.toString(), ",");
            MonthDoWWritable mw = new MonthDoWWritable();
            mw.month = new IntWritable(Integer.parseInt(s[0]));
            mw.dayOfWeek = new IntWritable(Integer.parseInt(s[1]));
            context.write(mw, new Text(s[2]));
        }
    }

    public static class SortReducer extends
            Reducer<MonthDoWWritable, Text, Text, Text> {
        public void reduce(MonthDoWWritable key,
                           Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            *//*Iterator it = values.iterator();
            while (it.hasNext()) {
                String data = it.next().toString();
                System.out.println(data);
                context.write(new Text(String.valueOf(key.month)+","+String.valueOf(key.dayOfWeek)), new Text(","+data));
            }*//*
            for (Text val : values) {
                context.write(new Text(String.valueOf(key.month)+","+String.valueOf(key.dayOfWeek)), new Text(","+val));
            }
        }
    }*/
}
