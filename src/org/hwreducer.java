package org;

import java.io.IOException;
import java.util.HashSet;
import java.util.StringJoiner;
// import org.apache.commons.logging.Log;
// import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.Text;



/**
 * Created by shara on 4/08/2017.
 */

public class hwreducer extends Reducer<Text,Text,Text,Text> {
    // The Karmasphere Studio Workflow Log displays logging from Apache Commons Logging, for example:
    // private static final Log LOG = LogFactory.getLog("org.smaple.HomeworkReducer");

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        // TODO: please implement your reducer here

        HashSet<String> newhasset;
        String temptext;
        newhasset = new HashSet<>();
        StringJoiner finalstring;
        finalstring = new StringJoiner(",");

        for (Text text:values)
        {

            temptext = text.toString();
            boolean add = newhasset.add(temptext);
            if (add)
            {
                System.out.println("");
            }

        }
        /* StringBuilder b=new StringBuilder(",");
        for (int i=0;i<b.length();i++){
            b.insert(i,valuesArray);
*/
        for (String k: newhasset ) finalstring.add(k);
        //Text newtext=new Text(finalstring.toString());
        context.write(key,new Text(finalstring.toString()));
    }
}
