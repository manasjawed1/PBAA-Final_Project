
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

        // This is for the income file, and we need the fields:
        // zipcode (main thing), state (to verify zipcode), AGI_STUB (tax bracket), N1 (number of returns), 
        //A02650 (total income amount), A18425 (State and local income taxes amount), 
        //A18500 (real estate taxes amount), N18300 (number of people who paid taxes)
        // , A18300 (taxes paid amount), A04800 (taxable income amount)

        String zipcode = line.split(",")[2];
        String start = "0";
        String end = "99999";

        if( (
                (zipcode.compareTo(start) > 0 && zipcode.compareTo(end) < 0)
                )
                ){

            String state = line.split(",")[1];
            String agi_stub = line.split(",")[3];
            String n1 = line.split(",")[4];
            String total_income = line.split(",")[22];
            String state_and_local = line.split(",")[70];
            String real_estate = line.split(",")[74];
            String num_paid = line.split(",")[79];
            String paid_amount = line.split(",")[80];
            String taxable_amount = line.split(",")[96];

            // to_write determines the format
            String to_write = zipcode + "," + state + "," + agi_stub + "," + n1 + "," + total_income + "," + state_and_local + "," + real_estate + "," + num_paid + "," + paid_amount + "," + taxable_amount;
            Text outputKey = new Text(to_write);
            IntWritable outputValue = new IntWritable(1);
            con.write(outputKey, outputValue);

        }


        


    }
}
