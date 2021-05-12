
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class CleanMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
    public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException
    {

    	String line = value.toString();
    	int count = line.split(",").length;

        // This is for the real estate file, and we need the fields:
        // month_date_yyyymm, postal_code, zip_name, median_listing_price, 
        // active_listing_count, median_days_on_market, median_listing_price_per_square_foot, 
        // median_square_feet, average_listing_price, total_listing_count

        String [] tokens = line.split(",");
        

        if (count > 34){

            String date = tokens[0];
            // String zip_name = line.split(",")[2];
            String median_listing_price = tokens[5];
            String active_listing_count = tokens[8];
            // String median_days_on_market = tokens[10];
            String median_listing_price_per_square_foot = tokens[26];
            String median_square_feet = tokens[29];
            String average_listing_price = tokens[32];
            String total_listing_count = tokens[35];
            
            String zipcode = tokens[1];

            String start = "201801";
            String end = "201812";

            // date should either give 0 with start or end, or give +ive with start and -ive with end

            if (!date.equals("month_date_yyyymm") && (
                (date.compareTo(start) == 0) || 
                (date.compareTo(end) == 0) || 
                (date.compareTo(start) > 0 && date.compareTo(end) < 0)
                )
                ){

            String to_write = date + "," + zipcode + "," +  median_listing_price + "," + active_listing_count;
            to_write += "," + median_listing_price_per_square_foot + "," + median_square_feet;
            to_write +=  "," + average_listing_price + "," + total_listing_count;
            Text outputKey = new Text(to_write.replace("*", ""));
            IntWritable outputValue = new IntWritable(1);
            con.write(outputKey, outputValue);

            }

            // else{
            // Text outputKey = new Text("");
            // IntWritable outputValue = new IntWritable(1);
            // con.write(outputKey, outputValue);
            // }
            

        }

        // Text outputKey = new Text(zipcode);
        // IntWritable outputValue = new IntWritable(1);
        // con.write(outputKey, outputValue);



        


    }
}
