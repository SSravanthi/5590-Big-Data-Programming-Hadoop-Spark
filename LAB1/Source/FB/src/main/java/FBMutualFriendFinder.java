import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;

public class FBMutualFriendFinder {

    public static void main(String[] args) throws Exception{
        JobConf conf = new JobConf(FBMutualFriendFinder.class);
        conf.setJobName("MutualFriendFinder");

        conf.setMapperClass(FBFriendMapper.class);
        conf.setReducerClass(FBFriendReducer.class);

        conf.setMapOutputKeyClass(Text.class);
        conf.setMapOutputValueClass(Text.class);

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(conf, new Path("input"));
        FileOutputFormat.setOutputPath(conf, new Path("output"));

        JobClient.runJob(conf);
    }
}