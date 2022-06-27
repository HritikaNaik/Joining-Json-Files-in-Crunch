import java.util.*;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.Path;

import org.apache.crunch.PCollection;
import org.apache.crunch.PTable;
import org.apache.crunch.Pipeline;
import org.apache.crunch.PipelineResult;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.types.writable.Writables;

import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class JsonFileJoiner extends Configured implements Tool{
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new JsonFileJoiner(), args);
        System.exit(res);
    }

    @Override
    public int run(String args[]){
        Configuration conf= getConf();
        conf.set("mapred.job.queue.name","d_bi");

        String outputPath = args[4];//The last command line argument is the output path
        cleanHDFSOutPath(new Path(outputPath));//Incase the output path already has a value from a previous iteration, it will have to be deleted

        //Input files make up n-1 of the n arguments given in command line, and so we read them as such
        String inputPath1 = args[0];
        String inputPath2 = args[1];
        String inputPath3 = args[2];
        String inputPath4 = args[3];

        Pipeline pipeline = new MRPipeline(JsonFileJoiner.class, getConf());//Make pipeline to take input to process

        PCollection<String> data1 = pipeline.readTextFile(inputPath1);//One by one read data from each address given in arguments
        PCollection<String> data2 = pipeline.readTextFile(inputPath2);
        PCollection<String> data3 = pipeline.readTextFile(inputPath3);
        PCollection<String> data4 = pipeline.readTextFile(inputPath4);

        PCollection<String> data = data1.union(data2,data3,data4);//Unite all the data into one collection

        PTable<String,String> id_pairs = data.parallelDo(new MakePairs(),Writables.tableOf(Writables.strings(),Writables.strings()));//Make key value pairs with product id and feature

        PTable<String, Collection<String>> dataColl = id_pairs.collectValues();//Aggregate all values with the same product id into one composite value

        PCollection<String> fin_data = dataColl.parallelDo(new JoinFiles(), Writables.strings());//Write into required format for output

        pipeline.writeTextFile(fin_data, outputPath);//Give final data to output address from arguments

        PipelineResult result = pipeline.done();
        return result.succeeded() ? 0 : 1;
    }
    private void cleanHDFSOutPath(Path pathToDelete) {
        try {
            Configuration conf = getConf();
            FileSystem hdfs = FileSystem.get(URI.create(pathToDelete.toString()),conf);
            if (hdfs.exists(pathToDelete)) {
                hdfs.delete(pathToDelete, true);
            }
        } catch (Exception exp) {
            exp.fillInStackTrace();
        }
    }
}
