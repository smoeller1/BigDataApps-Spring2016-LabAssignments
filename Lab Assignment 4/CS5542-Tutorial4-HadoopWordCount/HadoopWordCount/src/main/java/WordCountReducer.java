/**
 * Created by Mayanka on 03-Sep-15.
 */
import java.io.IOException;
import java.util.Vector;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.omg.CORBA.Context;

public class WordCountReducer extends
        Reducer<Object[], Object[], Object[], Vector<Object>> {

    //public void reduce(Text text, Iterable<IntWritable> values, Context context)
    public void reduce(Object[] key, Iterable<Object[]> values, Context context)
            throws IOException, InterruptedException {
        for (Object[] val1 : values) {
            Vector<Object> join = new Vector<Object>();
            for(Object[] val2 : values) {
                if (val1 == val2) {
                    join.add(val1);
                }
            }
            context.write(key, join);
        }
    }
}