import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

public class FBFriendReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text>{
    public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter)
            throws IOException{

        List<String> outList = new LinkedList<String>();
        StringBuilder outStringBuild = new StringBuilder();
        Text[] inValues = new Text[2];

        //read values from (key : value) pairs into an array
        int i = 0;
        while(values.hasNext()){
            inValues[i++] = new Text(values.next());
        }

        String[] list1 = inValues[0].toString().split(",");
        String[] list2 = inValues[1].toString().split(",");

        //build list
        for(String friend1 : list1){
            for(String friend2 : list2){
                if(friend1.equals(friend2)){
                    outList.add(friend1);
                }
            }
        }

        //convert list to comma seperated string
        for(i = 0; i < outList.size(); i++){
            outStringBuild.append(outList.get(i));
            if(i != outList.size() - 1)
                outStringBuild.append(",");
        }

        output.collect(key, new Text(outStringBuild.toString()));
    }
}