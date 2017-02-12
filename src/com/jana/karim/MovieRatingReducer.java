package com.jana.karim;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.v2.proto.MRProtos;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

/**
 * Created by hdadmin on 10/24/16.
 */
public class MovieRatingReducer extends Reducer<Text, Text, Text, Text> {
    String user_id="";
    String user_rating="";
    String movie_title = "";
    String release_date ="";
    String imbd_url = "";
    float avg_rating = 0;
    Set<String> users = new HashSet<String>();
    int total_ratings=0;
    float users_rating = 0;
    String output_data ="";
    Text output_record = new Text();
    Logger logger = Logger.getLogger(MovieRatingReducer.class);

    public String print_arr(String[] result){
        String output="";
        for(String r : result){
            output += r + ", ";
        }
        output += "\n";
        return output;
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        logger.debug("DEBUG - IN REDUCER SETUP");
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        avg_rating = 0;
        total_ratings = 0;
        users_rating = 0;
        users.clear();
        logger.debug("DEBUG - PROCESS A KEY & ITERABLE VALUE");
        for(Text val: values){
            String line = val.toString();
            String[] result = line.split("\\|");

            if(result.length == 2){
                user_id = result[0];
                user_rating = result[1];
                //list of unique users
                users.add(user_id);
                //list of numeric user_rating values
                users_rating += Integer.parseInt(user_rating);
                total_ratings += 1;
                avg_rating = users_rating/total_ratings;
            }
            else if (result.length == 3){

                movie_title = result[0];
                release_date = result[1];

                imbd_url = result[2];
            }
            else{
                System.out.println("Logging reducer error: unexpected array size - "+ print_arr(result));
            }
        }

        output_data = movie_title +"|"+ release_date +"|"+ imbd_url
                +"|"+ String.format("%.2f",avg_rating) +"|"+ String.valueOf(users.size())
                + "|"+String.valueOf(total_ratings);

        output_record.set(output_data);
        context.write(key,output_record);

        Counter counter = context.getCounter("MyReducerCounter", "UNIQUE_MOVIES");
        //Each processed key is a unique movie
        counter.increment(1);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);
        logger.debug("DEBUG - IN MAPPER CLEANUP");
    }


}