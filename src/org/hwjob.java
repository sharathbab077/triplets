package org;

// import org.apache.commons.logging.Log;
// import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.File;
import java.io.IOException;
import org.apache.commons.io.FileUtils;



/**
 * Created by shara on 4/08/2017.
 */
public class hwjob {
    // The Karmasphere Studio Workflow Log displays logging from Apache Commons Logging, for example:
    // private static final Log LOG = LogFactory.getLog("org.smaple.HomeworkJob");

    public static void main(String[] args) throws Exception {
        System.out.println("ASSIGNMENT 2-MAP REDUCE");
        System.out.println("Deleting the output folders");

        String path=args[1];
        deleteDirectory(path);
        //FileUtils.deleteDirectory(new File(path));
        Job job = new Job();

        /* Autogenerated initialization. */
        initJob(job);

        /* Custom initialization. */
        initCustom(job);

        /* Tell Task Tracker this is the main */
        job.setJarByClass(hwjob.class);

        /* This is an example of how to set input and output. */
        FileInputFormat.setInputPaths(job, args[0]);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        /* You can now do any other job customization. */
        // job.setXxx(...);

        /* And finally, we submit the job. */
        job.submit();

        job.waitForCompletion(true);
    }

    private static void deleteDirectory(String delpath) {
        //String SRC_FOLDER = "data\\output";
        String SRC_FOLDER=delpath;
        File directory = new File(SRC_FOLDER);
        //make sure directory exists
        if(!directory.exists()){
            System.out.println("Directory does not exist.");
            //System.exit(0);
        }else{
            try{
                delete(directory);
            }catch(IOException e){
                e.printStackTrace();
              //  System.exit(0);
            }
        }

      //  System.out.println("Done");


    }
    public static void delete(File directory)
            throws IOException{
        if(directory.isDirectory()){
            //directory is empty, then delete it
            if(directory.list().length==0){
                directory.delete();
                System.out.println("Output Folder is deleted : "
                        + directory.getAbsolutePath());
            }else{

                String files[] = directory.list();
                for (String temp : files) {
                    File fileDelete = new File(directory, temp);
                    //recursive delete
                    delete(fileDelete);
                }
                //check the directory again, if empty then delete it
                if(directory.list().length==0){
                    directory.delete();
                    System.out.println("Directory is deleted : "
                            + directory.getAbsolutePath());
                }
            }
        }else{

            directory.delete();
            System.out.println("File is deleted : " + directory.getAbsolutePath());
        }
    }



    /**
     * This method is executed by the workflow
     */
    public static void initCustom(Job job) {
        // Add custom initialisation here, you may have to rebuild your project before
        // changes are reflected in the workflow.
    }

    /** This method is called from within the constructor to
     * initialize the job.
     * WARNING: Do NOT modify this code. The content of this method is
     * always regenerated by the Job Editor.
     */
    @SuppressWarnings("unchecked")
    // <editor-fold defaultstate="collapsed" desc="Generated Code">//GEN-BEGIN:initJob
    public static void initJob(Job job) {
        org.apache.hadoop.conf.Configuration conf = job.getConfiguration();
// Generating code using Karmasphere Protocol for Hadoop 0.20
// CG_GLOBAL

// CG_INPUT_HIDDEN
        job.setInputFormatClass(org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat.class);

// CG_MAPPER_HIDDEN

        job.setMapperClass(org.hwmapper.class);
        job.getConfiguration().set("mapred.mapper.new-api", "true");

// CG_MAPPER
        job.getConfiguration().set("mapred.map.tasks", "3");
        job.setMapOutputKeyClass(org.apache.hadoop.io.Text.class);
        job.setMapOutputValueClass(org.apache.hadoop.io.Text.class);

// CG_PARTITIONER_HIDDEN
        job.setPartitionerClass(org.apache.hadoop.mapreduce.lib.partition.HashPartitioner.class);

// CG_PARTITIONER

// CG_COMPARATOR_HIDDEN

// CG_COMPARATOR

// CG_COMBINER_HIDDEN

// CG_REDUCER_HIDDEN

        job.setReducerClass(org.hwreducer.class);
        job.getConfiguration().set("mapred.reducer.new-api", "true");

// CG_REDUCER
        job.getConfiguration().set("mapred.reduce.tasks", "2");
        job.setOutputKeyClass(org.apache.hadoop.io.Text.class);
        job.setOutputValueClass(org.apache.hadoop.io.Text.class);

// CG_OUTPUT_HIDDEN
        job.setOutputFormatClass(org.apache.hadoop.mapreduce.lib.output.TextOutputFormat.class);

// CG_OUTPUT

// Others
        job.getConfiguration().set("", "");
    }

    // </editor-fold>//GEN-END:initJob

}
