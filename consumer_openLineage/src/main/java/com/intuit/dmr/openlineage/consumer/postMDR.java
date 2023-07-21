//all done
package com.intuit.dmr.openlineage.consumer;
import org.json.simple.*;
//import org.json.JSONArray;
//import org.json.JSONObject;
import java.util.logging.Logger;
import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
public class postMDR {
    public static final Logger logger= java.util.logging.Logger.getLogger(postMDR.class.getName());
    //This function only handles the sending of messages, response codes, and retrying if need be

    public static void POST (JSONObject payload, URL url) throws java.io.IOException
    {
        logInit.initializeLogger();
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        logger.finer("Getting the Authorization token for posting to DMR");
        System.out.println("Getting the Authorization token for posting to DMR");
        String token = getToken.token();
        logger.finer("Got the authorization ticket");
        conn.setRequestProperty("Authorization", token);
        conn.setRequestProperty("Content-Type", "application/json");
        conn.setRequestMethod("PUT");
        conn.setDoOutput(true);
        conn.setConnectTimeout(3000);
        logger.info(String.format("The payload created for posting is:\n%s\n",payload));
        System.out.println("The payload created for posting is: \n%s\n"+payload);
        DataOutputStream outputStream = new DataOutputStream(conn.getOutputStream());
        //sending the message
        outputStream.writeBytes(payload.toString());
        outputStream.flush();
        int code=conn.getResponseCode();
        logger.info("The response code:"+code);
        InputStream inputstream=conn.getInputStream();
        BufferedReader render=new BufferedReader(new InputStreamReader(inputstream));
        StringBuilder response=new StringBuilder();
        String line;
        while((line= render.readLine())!=null){
            response.append(line);
        }
        String responseBody=response.toString();
        logger.info("The response body: \n"+responseBody);
    }


    public static void postAtlas(JSONObject payload)
    {
        try
        {
            URL url = new URL("https://metadataregistry-e2e.api.intuit.com/v2/dataregistry/entities");
            try {
                POST(payload, url);
            }
            catch(java.io.IOException e){
                //incase we have to handle a particular exception, call the POST method
                logger.severe(e.toString());
            }
        }
        catch (java.net.MalformedURLException e)
        {
            //malfunctioned url
            logger.severe(e.toString());
        }
    }
}
