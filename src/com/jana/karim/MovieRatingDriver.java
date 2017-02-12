package com.jana.karim;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.yarn.util.SystemClock;
import org.apache.log4j.Logger;
import org.apache.log4j.Level;

import java.io.IOException;
import java.util.Iterator;



/**
 * @Data u.data & u.item
 **/
public class MovieRatingDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Path movielens_data_set = new Path(args[1]);
        Path results_destination = new Path(args[2]);

        System.out.println(movielens_data_set.toString() + "  " + results_destination.toString());
        Job job;

        Configuration conf = new Configuration();
        conf.set("mapreduce.map.log.level", "DEBUG");
        conf.set("mapreduce.reduce.log.level", "DEBUG");
        conf.set("mapreduce.framework.name", "local");

        //conf.setStrings("io.compression.codecs",
         //       "org.apache.hadoop.io.compress.GzipCodec");

        conf.setBoolean("mapreduce.output.fileoutputformat.compress",true);
        conf.setStrings("mapreduce.output.fileoutputformat.compress.codec",
                "org.apache.hadoop.io.compress.GzipCodec");

        conf.setBoolean("mapreduce.compress.map.output", true);
        conf.setStrings("mapreduce.map.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec");

        System.out.println(conf);
        System.out.println(conf.get("fs.default.name"));

        try {
            job = Job.getInstance(conf, "MovieRating");
        } catch (IOException ex){
            Logger.getLogger(MovieRatingDriver.class.getName()).log(Level.FATAL, null, ex);
            return;
        }

        TextInputFormat.addInputPath(job, movielens_data_set);
        job.setInputFormatClass(TextInputFormat.class);
        job.setMapperClass(MovieRatingMapper.class);
        job.setReducerClass(MovieRatingReducer.class);
        job.setJarByClass(MovieRatingDriver.class);

        FileOutputFormat.setOutputPath(job, results_destination);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setNumReduceTasks(2);
        job.waitForCompletion(true);

        System.out.println("Job is complete! Printing Counters:");

        Counters counters = null;
        counters = job.getCounters();

        for (String groupName : counters.getGroupNames()){
            CounterGroup group = counters.getGroup(groupName);
            System.out.println(group.getDisplayName());

            Iterator<Counter> groupCounters = group.iterator();
            while (groupCounters.hasNext()){
                Counter counter = groupCounters.next();
                System.out.println(" " + counter.getDisplayName() + "=" + counter.getValue());
            }
        }
    }
}
