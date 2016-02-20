/**
 * Created by Mayanka on 03-Sep-15.
 */
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.omg.CORBA.Context;

public class WordCountMapper extends
        Mapper<Object, Object[], Object[], Object[]> {

    //private final IntWritable ONE = new IntWritable(1);
    //private Text word = new Text();

    public void map(Object key, Object[] value, Context context)
            throws IOException, InterruptedException {

        /* Input:
           key = Facebook user object
           value = array of facebook users who are friends with key
           Output:
           key = Array of 2 facebook objects
           value = Array of mutual friend objects
         */
        for(int i = 0; i < value.length; i++) {
            Object[] newKeyTmp = new Object[2];
            Object[] newValue = new Object[value.length - 1];
            newKeyTmp[0] = key;
            int newValueCount = 0;
            for(int j = 0; j < value.length; j++) {
                if (i == j) {
                    newKeyTmp[1] = value[j];
                } else {
                    newValue[newValueCount++] = value[j];
                }
            }
            Object[] newKey = new Object[2];
            if (newKeyTmp[0] < newKeyTmp[1]) {
                newKey[0] = newKeyTmp[0];
                newKey[1] = newKeyTmp[1];
            } else {
                newKey[1] = newKeyTmp[0];
                newKey[0] = newKeyTmp[1];
            }
            context.write(newKey, newValue);
        }
        /*
        String[] csv = value.toString().split(" ");
        for (String str : csv) {
            word.set(str);
            context.write(word, ONE);
        } */
    }
}
