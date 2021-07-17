package com.geekbang;


import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FlowStatistics {

    public static class doMapper extends Mapper<Object, Text, FlowKey, Writable>{
        @Override
        protected void map(Object key, Text value, Context context)  throws IOException, InterruptedException {
            String phoneInfo = value.toString();//将输入的纯文本的数据转换成String
			//将输入的数据先按行进行分割
            StringTokenizer tokenizerArticle = new StringTokenizer(phoneInfo, "\n");
			//分别对每一行进行处理
            while(tokenizerArticle.hasMoreTokens()){
			// 每行按空格划分 
                StringTokenizer tokenizer = new StringTokenizer(tokenizerArticle.nextToken());
                String timestamp = tokenizer.nextToken();//时间戳
                String phoneNum = tokenizer.nextToken();//电话号码
                String mac = tokenizer.nextToken();//基站的物理地址
                String ip = tokenizer.nextToken();//访问网址的ip
                String domain = tokenizer.nextToken();//网站域名
                String dataNum = tokenizer.nextToken();//数据包
                String revPackage = tokenizer.nextToken();//接包数
                long upFlow = Long.parseLong(tokenizer.nextToken());//上行/传流量
                long downFlow = Long.parseLong(tokenizer.nextToken());//下行/传流量
                FlowKey phone = new FlowKey(phoneNum);
                FlowBean flowBean = new FlowBean(upFlow, downFlow);
                context.write(phone,flowBean);
            }
        }
    }

    public static class doReducer extends Reducer<FlowKey, Writable, FlowKey, IntWritable>{
        @Override
        protected void reduce(FlowKey key, Iterable<Writable> values,Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for(Writable value:values){
                FlowBean flowBean =  (FlowBean) value;
                sum += flowBean.getSumFlow();
            }
            context.write(key, new IntWritable(sum));
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException,InterruptedException {
        Job job = Job.getInstance();
        job.setJobName("iphonestatistics");
        Path in = new Path("hdfs://localhost:9000/user/orville/input/HTTP_20130313143750.dat");
        Path out = new Path("hdfs://localhost:9000/user/orville/output/out");

        FileInputFormat.addInputPath(job, in);
        FileOutputFormat.setOutputPath(job, out);

        job.setJarByClass(FlowStatistics.class);

        job.setMapperClass(doMapper.class);
        job.setReducerClass(doReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        System.exit(job.waitForCompletion(true)?0:1);
        System.out.println("end");
    }
}
