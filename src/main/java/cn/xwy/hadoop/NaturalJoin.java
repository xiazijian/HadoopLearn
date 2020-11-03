package cn.xwy.hadoop;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

public class NaturalJoin {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        conf.set("leftTable",args[2]);
        conf.set("leftTableKey",args[3]);
        conf.set("rightTable",args[4]);
        conf.set("rightTableKey",args[5]);
        conf.set("splitCharacter",args[6]);
        Job job = Job.getInstance(conf,"join OP");
        job.setJarByClass(NaturalJoin.class);
        job.setMapperClass(NaturalJoin.Map.class);
        job.setReducerClass(NaturalJoin.Reduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(Integer.parseInt(args[7]));
        //Path inputpath = new Path("D:\\Codes\\IDEA\\hadoopLearn\\join_data");
        Path inputpath = new Path(args[0]);
        FileInputFormat.addInputPath(job,inputpath);
        //Path outputpath = new Path("D:\\Codes\\IDEA\\hadoopLearn\\join_output");
        Path outputpath = new Path(args[1]);
        FileOutputFormat.setOutputPath(job,outputpath);
        System.exit(job.waitForCompletion(true)?0:1);
    }
    public static class Map extends Mapper<Object, Text,Text,Text>{
        public void map(Object key,Text value,Context context) throws IOException,InterruptedException{
            Configuration conf = context.getConfiguration();
            String splitCharacter = conf.get("splitCharacter");
            String leftTable = conf.get("leftTable");
            int leftTableKey = Integer.parseInt(conf.get("leftTableKey"));
            String rightTable = conf.get("rightTable");
            int rightTableKey = Integer.parseInt(conf.get("rightTableKey"));

            String str = value.toString();
            String[] s = StringUtils.split(str,splitCharacter);
            String name = ((FileSplit)context.getInputSplit()).getPath().getName();
            //左表
            if(name.equals(leftTable)){
                StringBuilder stringBuilder = new StringBuilder();
                for(int i =0;i<s.length;i++){
                    if(i!= leftTableKey -1){
                        if(i!=s.length-1){
                            stringBuilder.append(s[i]);
                            stringBuilder.append(splitCharacter);
                        }else {
                            stringBuilder.append(s[i]);
                        }
                    }
                }

                context.write(new Text(s[leftTableKey -1]),new Text("1"+stringBuilder.toString()));
            }
            // 根据表名判断，右表
            else if(name.equals(rightTable)){
                StringBuilder stringBuilder = new StringBuilder();
                for(int i =0;i<s.length;i++){
                    if(i!= rightTableKey -1){
                        if(i!=s.length-1){
                            stringBuilder.append(s[i]);
                            stringBuilder.append(splitCharacter);
                        }else {
                            stringBuilder.append(s[i]);
                        }
                    }
                }
                context.write(new Text(s[rightTableKey -1]),new Text("2"+stringBuilder.toString()));
            }
        }
    }
    public static class Reduce extends Reducer<Text,Text,Text,Text>{
        public void reduce (Text key,Iterable<Text> values,Context context)throws IOException,InterruptedException{
            Configuration conf = context.getConfiguration();
            String splitCharacter = conf.get("splitCharacter");

            ArrayList<String> leftTableList = new ArrayList<String>();
            ArrayList<String> rightTableList = new ArrayList<String>();
            Iterator it =  values.iterator();
            while (it.hasNext()){
                String data = it.next().toString();
                if(data.charAt(0)=='1'){//左表
                    leftTableList.add(data.substring(1));
                }
                else{
                    rightTableList.add(data.substring(1));
                }
            }
            //!!!!!!!!!!!有个重大缺陷，使用ArrayList保存左右表，万一表数据很大，内存就会溢出！！！
            if(!leftTableList.isEmpty()&&!rightTableList.isEmpty()){
                for(int i = 0;i<leftTableList.size();i++){
                    for(int j =0;j<rightTableList.size();j++){
                        context.write(new Text(leftTableList.get(i)+splitCharacter),new Text(rightTableList.get(j)+splitCharacter+key.toString()));
                    }
                }
            }
        }
    }
}
