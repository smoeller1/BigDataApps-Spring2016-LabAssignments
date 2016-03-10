/**
 * Created by feichenshen on 3/1/16.
 */



import org.json.JSONObject;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;

public class Main {

    public static void main(String[] args) {
        String textToAnalyze = "Where is the nearest exit";

        try {
            String encodedText = textToAnalyze.replace(" ", "+");
            String urlString = "http://uclassify.com/browse/uClassify/sentiment/ClassifyText?readkey=k6fuJ9R2HyKa&text="+encodedText+"%3f&output=json&version=1.01";
            //System.out.println("Using URL: "+urlString);
            URL url = new URL(urlString);
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("GET");
            conn.setRequestProperty("Accept", "application/json");

            if (conn.getResponseCode() != 200) {
                throw new RuntimeException("Failed : HTTP error code : "
                        + conn.getResponseCode());
            }

            BufferedReader br = new BufferedReader(new InputStreamReader(
                    (conn.getInputStream())));

            String jsonString = "";
            String output;
            //System.out.println("Output from Server .... \n");
            while ((output = br.readLine()) != null) {
                //System.out.println(output);
                jsonString += output;
            }

            conn.disconnect();

            JSONObject response = new JSONObject(jsonString);
            JSONObject cls1 = new JSONObject(response.get("cls1").toString());

            System.out.print("The phrase '"+textToAnalyze+"' is a ");
            if (Double.parseDouble(cls1.get("positive").toString()) >= 0.5) {
                System.out.println("positive phrase");
            } else {
                System.out.println("negative phrase");
            }

        } catch (MalformedURLException e) {

            e.printStackTrace();

        } catch (IOException e) {

            e.printStackTrace();

        }

    }

}
