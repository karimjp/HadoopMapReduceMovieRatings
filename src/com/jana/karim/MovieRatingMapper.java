package com.jana.karim;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * Created by hdadmin on 10/24/16.
 */
public class MovieRatingMapper extends Mapper<LongWritable, Text, Text, Text> {
    Text item_id = new Text();
    Text ratings_by_user = new Text();
    Text movie_info = new Text();
    Logger logger = Logger.getLogger(MovieRatingMapper.class);

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        logger.debug("DEBUG - IN MAPPER SETUP");
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        logger.debug("DEBUG - PROCESS A RECORD");
        FileSplit fileSplit = (FileSplit) context.getInputSplit();
        String filename = fileSplit.getPath().getName();
        String line = value.toString();
        //System.out.println(line);
        if(filename.contains(".data")){
            String[] result = line.split("\t");
            //System.out.println(line + "==" +result.length);
            item_id.set(result[1]);
            String data = result[0] + "|" + result[2];
            ratings_by_user.set(data);
            context.write(item_id, ratings_by_user);
        }
        if(filename.contains(".item")){
            String[] result = line.split("\\|");
            //System.out.println(line + "==" + result.length);
            item_id.set(result[0]);
            System.out.println("Result 4: " + result[4]);
            if(result[4].isEmpty()){
                result[4] = "Not available";
            }
            String data = result[1] +"|" + result[2] + "|" + result[4];
            //System.out.println(data + "==" + result.length);
            movie_info.set(data);
            context.write(item_id, movie_info);

        }

        Counter counter = context.getCounter("MyMapperCounter", "NUM_RECORDS");
        //Each processed key maps to a record
        counter.increment(1);


    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);
        logger.debug("DEBUG - IN MAPPER CLEANUP");
    }
}
