package org;

import java.io.File;
import java.io.IOException;
import java.util.*;

// import org.apache.commons.logging.Log;
// import org.apache.commons.logging.LogFactory;
//import com.sun.xml.internal.xsom.impl.scd.Iterators;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.Text;

import static com.sun.org.apache.xalan.internal.lib.ExsltStrings.split;



/**
 * Created by shara on 4/08/2017.
 */
public class hwmapper extends Mapper<Text,Text,Text,Text> {
    // The Karmasphere Studio Workflow Log displays logging from Apache Commons Logging, for example:
    // private static final Log LOG = LogFactory.getLog("org.smaple.HomeworkMapper");

    private Map<Text, List<Text>> oldvaluemap;
    private List<Text> newvaluelist;

    public hwmapper() {
        oldvaluemap = new HashMap<Text, List<Text>>();
        newvaluelist= new ArrayList<>();

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
    }
    public static void delete(File directory)
            throws IOException{
        if(directory.isDirectory()){
            //directory is empty, then delete it
            if(directory.list().length==0){
                directory.delete();
                System.out.println("Output Folder is deleted : "+ directory.getAbsolutePath());
            }else{

                String files[] = directory.list();
                for (String temp : files) {
                    File fileDelete = new File(directory, temp);
                    //recursive delete
                    delete(fileDelete);
                }
                if(directory.list().length==0){
                    directory.delete();
                    System.out.println("Directory is deleted : "+ directory.getAbsolutePath());
                }
            }
        }else{

            directory.delete();
            System.out.println("File is deleted : " + directory.getAbsolutePath());
        }
    }


    @Override
    protected void map(Text key, Text value, Context context)
            throws IOException, InterruptedException {
        // TODO: please implement your mapper code here
        System.out.println("Implemented deleting output folder in job file ");
        //doesnt delete the directory here so need to delete the folder before job
       // deleteDirectory();
        String[] new_string;
        String temptext1;
        Text temptext;
        int k;
        key.toString().trim();
        new Text(value.toString().trim());
        if (key.getLength() > 0 && value.getLength() > 0) {
            if (oldvaluemap.containsKey(key))
            {
                oldvaluemap.get(key).add(new Text(value)); //1,2
            }
            else
            {
                oldvaluemap.put(new Text(key), new ArrayList<>());
                oldvaluemap.get(key).add(new Text(value));
            }
            temptext = new Text(value + "," + key);//value 2,1
            newvaluelist.add(temptext);

            k = newvaluelist.size();
            for (int i = 0; i < k; i++)
            {
                temptext1 = newvaluelist.get(i).toString();
                new_string = temptext1.split(",");
                    for (Iterator<Text> iterator = oldvaluemap.keySet().iterator(); iterator.hasNext(); ) {
                        Text keys = iterator.next();
                        for (Text values : oldvaluemap.get(keys))
                            if (!keys.toString().equalsIgnoreCase(new_string[1]) && values.toString().equals(new_string[1]) && !keys.toString().equalsIgnoreCase(new_string[0]) && !new_string[1].equalsIgnoreCase(new_string[0]))
                                context.write(new Text(temptext1), keys);
                }}}}}
