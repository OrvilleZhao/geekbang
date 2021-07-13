package com.geekbang;


import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class App implements Tool{
    public static final Logger log=LoggerFactory.getLogger(App.class);
    Configuration configuration;
    public static class MyMap extends Mapper<Object, Text, Text, Writable>{
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
                Text phone = new Text(phoneNum);
                FlowBean flowBean = new FlowBean(upFlow, downFlow);
                context.write(phone,flowBean);
            }
        }
    }

    public static class MyReduce extends Reducer<Text, Writable, Text, IntWritable>{
 
        @Override
        protected void reduce(Text key, Iterable<Writable> values,Context context)
                throws IOException, InterruptedException {
            int sum=0;
            Iterator<Writable> iterator =  values.iterator();
            while(iterator.hasNext()){
                FlowBean flowBean = ((FlowBean) iterator.next());//计算总分
                sum += flowBean.getSumFlow();
            }
            context.write(key,new IntWritable(sum));//输出学生姓名和平均值
        }
        
    }

    @Override
    public Configuration getConf() {
        // TODO Auto-generated method stub
        return null;
    }
    @Override
    public void setConf(Configuration arg0) {
        // TODO Auto-generated method stub
        
    }
    @Override
    public int run(String[] arg0) throws Exception {
        // TODO Auto-generated method stub
        return 0;
    }
}
