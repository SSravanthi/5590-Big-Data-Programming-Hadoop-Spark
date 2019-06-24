import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.Arrays;
import java.util.StringTokenizer;

public class FBFriendMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text>{
    public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter)
            throws IOException{
        String line;
        String[] userFriendsArray;
        String[] friendArray;
        String[] tempKeyValue;

        //tokenize the values from (key : value) pairs, to iterate over
        StringTokenizer inTokens = new StringTokenizer(value.toString(), "\n");


        //iterate over each line (as tokens) from input, split input and friends list (at "-"), then split friends list
        //into individual friends (at ","), then iterate over friends list building (key : value) pairs
        while(inTokens.hasMoreTokens()){
            line = inTokens.nextToken();
            userFriendsArray = line.split("-"); //split input from their friends list
            String user = userFriendsArray[0];
            friendArray = userFriendsArray[1].split(","); //split friends list into individual friends

            //iterate over friends list, creating (key:value) pairs with input
            tempKeyValue = new String[2];
            for(int i = 0; i < friendArray.length; i++){
                tempKeyValue[0] = friendArray[i]; //set 'key' as friend from users friend list
                tempKeyValue[1] = user; //set 'value' as input
                Arrays.sort(tempKeyValue);
                output.collect(new Text(tempKeyValue[0] + " " + tempKeyValue[1]), new Text(userFriendsArray[1]));
            }
        }
    }
}