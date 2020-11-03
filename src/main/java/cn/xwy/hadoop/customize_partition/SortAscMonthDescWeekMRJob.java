package cn.xwy.hadoop.customize_partition;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.Iterator;

public class SortAscMonthDescWeekMRJob extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        String arg = "-D mapreduce.job.reduces=10 ./input/customize_partition ./output/customize_partition";
        String[] Args = arg.split(" ");
        ToolRunner.run(new SortAscMonthDescWeekMRJob(), Args);
    }

    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(getConf(),"SortAscMonthDescWeekMRJob");
        job.setJarByClass(SortAscMonthDescWeekMRJob.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);


        job.setMapOutputKeyClass(MonthDoWWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(MonthDoWWritable.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(SortAscMonthDescWeekMapper.class);
        job.setReducerClass(SortAscMonthDescWeekReducer.class);
        job.setPartitionerClass(MonthDoWPartitioner.class);



        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
        return 0;
    }

    public static class SortAscMonthDescWeekMapper extends
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

    public static class SortAscMonthDescWeekReducer extends
            Reducer<MonthDoWWritable, Text, Text, Text> {
        public void reduce(MonthDoWWritable key,
                           Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            /*Iterator it = values.iterator();
            while (it.hasNext()) {
                String data = it.next().toString();
                System.out.println(data);
                context.write(new Text(String.valueOf(key.month)+","+String.valueOf(key.dayOfWeek)), new Text(","+data));
            }*/
            System.out.println(key.month);
            System.out.println(key.dayOfWeek);
            System.out.println("============");
            for (Text val : values) {
                context.write(new Text(String.valueOf(key.month)+","+String.valueOf(key.dayOfWeek)), new Text(","+val));
            }
        }
    }

    public static class MonthDoWPartitioner extends
            Partitioner<MonthDoWWritable, Text> implements Configurable {
        private Configuration conf = null;
        private int indexRange = 0;

        private int getDefaultRange() {
            int minIndex = 0;
            int maxIndex = 11 * 7 + 6;
            int range = (maxIndex - minIndex) + 1;
            return range;
        }

        @Override
        public void setConf(Configuration configuration) {
            this.conf = configuration;
            //如果配置中有key.range这个配置就使用这个配置的值，没有就使用后面这个getDefaultRange()的值
            this.indexRange = conf.getInt("key.range", getDefaultRange());
        }

        @Override
        public Configuration getConf() {
            return this.conf;
        }

        @Override
        public int getPartition(MonthDoWWritable key, Text value, int numReduceTasks) {
            int indicesPerReducer = (int) Math.floor(indexRange / numReduceTasks);
            int index = (key.month.get() - 1) * 7 + (7 - key.dayOfWeek.get());
            //如果索引的范围比reduce数量少，直接返回索引值
            if (indexRange < numReduceTasks) {
                return index;
            }
            //索引范围比reduce数量多就开始对reduce进行索引范围的分配，最后剩下的都给最后一个reduce
            for (int i = 0; i < numReduceTasks; i++) {
                int minValForPartitionInclusive = (i) * indicesPerReducer;
                int maxValForParitionExclusive = (i + 1) * indicesPerReducer;

                if (index >= minValForPartitionInclusive
                        && index < maxValForParitionExclusive) {
                    return i;
                }
            }
            /*
             * 最后剩余没有分配的都交给最后一个reduce处理
             */
            return (numReduceTasks - 1);
        }


    }
}
