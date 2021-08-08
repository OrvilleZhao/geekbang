package com.orville;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.regex.Pattern;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobConfigurable;
import org.apache.hadoop.mapred.LineRecordReader;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;

public class GeekTextInputFormat implements
    InputFormat<LongWritable, BytesWritable>, JobConfigurable {
    
    public static class GeekLineRecordReader implements
    RecordReader<LongWritable, BytesWritable>, JobConfigurable {
        LineRecordReader reader;
        Text text;

        public GeekLineRecordReader(LineRecordReader reader) {
            this.reader = reader;
            text = reader.createValue();
        }

        @Override
        public void close() throws IOException {
            reader.close();
        }

        @Override
        public LongWritable createKey() {
         return reader.createKey();
        }

        @Override
        public BytesWritable createValue() {
         return new BytesWritable();
        }

        @Override
        public long getPos() throws IOException {
            return reader.getPos();
        }

        @Override
        public float getProgress() throws IOException {
            return reader.getProgress();
        }

        @Override
        public boolean next(LongWritable key, BytesWritable value) throws IOException {
            while (reader.next(key, text)) {
                // text -> byte[] -> value
                byte[] textBytes = text.getBytes();
                int length = text.getLength();

                // Trim additional bytes
                if (length != textBytes.length) {
                    textBytes = Arrays.copyOf(textBytes, length);
                }
                int j;
                for (j = 0; j < textBytes.length && j < signature.length
                    && textBytes[j] == textBytes[j]; ++j) {
                    ;
                }

                // return the row only if it's not corrupted
                if (j == signature.length) {
                    String inputText = new String(textBytes);
                    String[] paragraph = inputText.split(" ");
                    String pattern = "ge{2,256}k";
                    String[] result = Arrays.copyOf(paragraph, paragraph.length);
                    int count = 1;
                    for (int i=0;i<paragraph.length;i++) {
                        boolean isMatch = Pattern.matches(pattern, paragraph[i]);
                        //如果是gee...k这种
                        if (isMatch){
                            result = Arrays.copyOf(result, result.length-1);
                            System.arraycopy(paragraph, i+1, result, i+1-count, paragraph.length-i-1);
                            count += 1;
                        }
                    }
                    StringBuilder builder = new StringBuilder();
                    for(int i =0;i<result.length;i++){
                            builder.append(result[i]);
                            if (i!=result.length-1) {
                                    builder.append(" ");
                            }
                    }
                    byte[] binaryData = builder.toString().getBytes();
                    value.set(binaryData, 0, binaryData.length);
                    return true;
                }
            }
            // no more data
            return false;
        }

        private byte[] signature;

        @Override
        public void configure(JobConf job) {
            String signatureString = job.get("base64.text.input.format.signature");
            if (signatureString != null) {
                signature = signatureString.getBytes(StandardCharsets.UTF_8);
            } else {
                signature = new byte[0];
            }
        }
    }

    TextInputFormat format;
    JobConf job;
    
    public GeekTextInputFormat(){
        format = new TextInputFormat();
    }

    @Override
    public void configure(JobConf job) {
        this.job = job;
        format.configure(job);
    }

    public RecordReader<LongWritable, BytesWritable> getRecordReader(
        InputSplit genericSplit, JobConf job, Reporter reporter) throws IOException {
        reporter.setStatus(genericSplit.toString());
        GeekLineRecordReader reader = new GeekLineRecordReader(
            new LineRecordReader(job, (FileSplit) genericSplit));
        reader.configure(job);
        return reader;
    }

    @Override
    public InputSplit[] getSplits(JobConf job, int numSplits) throws IOException {
        return format.getSplits(job, numSplits);
    }


}
