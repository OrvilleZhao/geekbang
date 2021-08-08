package com.orville;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobConfigurable;
import org.apache.hadoop.util.Progressable;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Properties;
import java.util.Random;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter;
import org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat;

/**
 * Hello world!
 */
public class GeekTextOutputFormat<K extends WritableComparable, V extends Writable>
        extends HiveIgnoreKeyTextOutputFormat<K, V> {
        /**
         * GeekRecordWriter.
         *
         */
        public static class GeekRecordWriter implements RecordWriter,JobConfigurable {
                RecordWriter writer;
                BytesWritable bytesWritable;

                public GeekRecordWriter(RecordWriter writer) {
                this.writer = writer;
                bytesWritable = new BytesWritable();
                }

                @Override
                public void write(Writable w) throws IOException {

                        // Get input data
                        byte[] input;
                        int inputLength;
                        if (w instanceof Text) {
                                input = ((Text) w).getBytes();
                                inputLength = ((Text) w).getLength();
                        } else {
                                assert (w instanceof BytesWritable);
                                input = ((BytesWritable) w).getBytes();
                                inputLength = ((BytesWritable) w).getLength();
                        }
        
                        String text = new String(input);
                        String[] paragraph = text.split(" ");
                        Random r = new Random();
                        //String pattern = "ge+k";
                        int threshold = r.nextInt(254)+2;
                        int tmpCount = 0;
                        String[] result = Arrays.copyOf(paragraph, paragraph.length);
                        int count = 1;
                        // 随机生成等2-256个单词加ge...k
                        for(int i=0; i<paragraph.length; i++){
                               tmpCount += 1;
                               if (tmpCount == threshold){
                                        result = Arrays.copyOf(result, result.length+1);
                                        System.arraycopy(paragraph, i+1, result, i+1+count, paragraph.length-i-1);
                                        StringBuilder builder = new StringBuilder("g");
                                        for (int j=0;j<threshold;j++){
                                                builder.append("e");
                                        }
                                        builder.append("k");
                                        result[i+1] = builder.toString();  
                                        count += 1;
                                        tmpCount = 0;
                                        threshold = r.nextInt(254)+2;
                               } 
                        }
                        StringBuilder builder = new StringBuilder();
                        for(int i =0;i<result.length;i++){
                                builder.append(result[i]);
                                if (i!=result.length-1) {
                                        builder.append(" ");
                                }
                        }
                        byte[] tmpWrapped = builder.toString().getBytes();
                        // Add signature
                        byte[] wrapped = new byte[signature.length +tmpWrapped.length];
                        for (int i = 0; i < signature.length; i++) {
                                wrapped[i] = signature[i];
                        }
                        
                        for (int i = 0; i < tmpWrapped.length; i++) {
                                wrapped[i + signature.length] = tmpWrapped[i];
                        }
                        bytesWritable.set(wrapped, 0, wrapped.length);
                        writer.write(bytesWritable);
                }

                @Override
                public void close(boolean abort) throws IOException {
                        writer.close(abort);
                }

                private byte[] signature;

                @Override
                public void configure(JobConf job) {
                        String signatureString = job.get("base64.text.output.format.signature");
                        if (signatureString != null) {
                                signature = signatureString.getBytes(StandardCharsets.UTF_8);
                        } else {
                                signature = new byte[0];
                        }
                }
        }

        @Override
        public RecordWriter getHiveRecordWriter(JobConf jc, Path finalOutPath,
                Class<? extends Writable> valueClass, boolean isCompressed,
                Properties tableProperties, Progressable progress) throws IOException {
                GeekRecordWriter writer = new GeekRecordWriter(super
                        .getHiveRecordWriter(jc, finalOutPath, BytesWritable.class,
                        isCompressed, tableProperties, progress));
                writer.configure(jc);
                return writer;
        }
}
