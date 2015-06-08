/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.michaeljones.hellohadoopworldmaven;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.commons.collections.iterators.ListIteratorWrapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author michaeljones
 */
public class HelloMapReduce {    
    private static final Logger LOGGER = LoggerFactory.getLogger(HelloMapReduce.class.getName());

    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        @Override
        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());
                context.write(word, one);
            }
        }
    }
    
    public static class HelloIntReducer {
        private boolean analysisIsOn = false;
        private String callersName = "";
        private final IntWritable result = new IntWritable();
        
        public HelloIntReducer() {
            analysisIsOn = false;
        }
        
        public HelloIntReducer(String className) {
            analysisIsOn = true;
            callersName = className;
        }
        
        private String makeWriteDump(Text key, ListIteratorWrapper values) {
            // For analysis, we append all the values to the key to see how
            // the result was arrived at.
            StringBuilder keyString = new StringBuilder();
            // Tells us whether it is the reducer or combiner which is performing
            // the reduce.
            keyString.append(callersName);
            keyString.append("-").append(key.toString()).append("[");

            boolean firstValue = true;
            while (values.hasNext()) {
                if (!firstValue) {
                    keyString.append(",");
                }
                
                keyString.append((IntWritable)values.next());
                firstValue = false;
            }
            
            keyString.append("]");
            
            return keyString.toString();
        }

        public void reduce(Text key, Iterable<IntWritable> values,
                Reducer.Context context
        ) throws IOException, InterruptedException {
            ListIteratorWrapper wrappedValues = new ListIteratorWrapper(values.iterator());
            if (analysisIsOn) {
                String writeDump = makeWriteDump(key, wrappedValues);
                LOGGER.info(writeDump);
                wrappedValues.reset();
            }
                
            int sum = 0;
            while(wrappedValues.hasNext()) {
                IntWritable val = (IntWritable)wrappedValues.next();
                sum += val.get();
            }
            
            result.set(sum);
            context.write(key, result);            
        }        
    }

    public static class IntSumReducer
            extends Reducer<Text, IntWritable, Text, IntWritable> {
        // Containment rather than inheritance.
        private final HelloIntReducer intReducer;

        public IntSumReducer() {
            // Simple reducer, no analysis dump.
            intReducer = new HelloIntReducer();
        }

        @Override
        public void reduce(Text key, Iterable<IntWritable> values,
                Context context
        ) throws IOException, InterruptedException {
            intReducer.reduce(key, values, context);
        }
    }
    
    public static class IntSumReducerAnalyser
            extends Reducer<Text, IntWritable, Text, IntWritable> {
        // Containment rather than inheritance.
        private final HelloIntReducer intReducer;

        public IntSumReducerAnalyser() {
            // Reducer will dump class name and analysis.
            intReducer = new HelloIntReducer(this.getClass().getSimpleName());
        }

        @Override
        public void reduce(Text key, Iterable<IntWritable> values,
                Context context
        ) throws IOException, InterruptedException {
            intReducer.reduce(key, values, context);
        }
    }

    public static class IntSumCombinerAnalyser
            extends Reducer<Text, IntWritable, Text, IntWritable> {
        private final HelloIntReducer intReducer;
        
        public IntSumCombinerAnalyser() {
            // Reducer will dump class name and analysis.
            intReducer = new HelloIntReducer(this.getClass().getSimpleName());
        }
        
        @Override
        public void reduce(Text key, Iterable<IntWritable> values,
                Context context
        ) throws IOException, InterruptedException {
            intReducer.reduce(key, values, context);
        }
    }

    public static Job RunJobAsync(
            Path inputPath,
            Path outputPath,
            Configuration conf) throws Exception {
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(HelloMapReduce.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);
        
        return job;
    }

    public static Job RunJobAnalysisAsync(
            Path inputPath,
            Path outputPath,
            Configuration conf) throws Exception {
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(HelloMapReduce.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumCombinerAnalyser.class);
        job.setReducerClass(IntSumReducerAnalyser.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);
        
        return job;
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = RunJobAsync(new Path(args[0]), new Path(args[1]), conf);
        
        // The orginal WordCount.java that comes with the distribution has a
        // System exit on job completion, but this makes it impossible to unit
        // test.
        boolean ok = job.waitForCompletion(true);
    }
    
}
