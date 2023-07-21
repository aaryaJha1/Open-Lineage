//all done

package com.intuit.dmr.openlineage.consumer;
import org.json.JSONArray;
import org.json.JSONObject;
import org.apache.logging.log4j.LogManager;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.io.DataOutputStream;
import java.io.InputStreamReader;
import java.io.BufferedReader;
import java.util.logging.Logger;
//"s3://idl-datacatalog2-data-lake-e2e/aarya/employee/One"
public class searchHive {
    //function to map the s3 location to hive table
    public static final Logger logger= java.util.logging.Logger.getLogger(app.class.getName());
    public static String s3ToHive(String loc) throws hiveMappingException
    {
        //creating request
        logInit.initializeLogger();
        JSONObject req = new JSONObject();

        req.put("includeRelationships", false);
        req.put("getHeadersOnly", true);
        req.put("ignoreReferredEntities", false);
        req.put("typeName", "hive_storagedesc");
        req.put("excludeDeletedEntities", false);
        req.put("includeClassificationAttributes", false);
        req.put("includeSubTypes", true);
        req.put("includeSubClassifications", true);
        req.put("limit", 1);
        req.put("offset", 0);

        JSONObject entityFilters = new JSONObject();
        entityFilters.put("condition", "AND");

        JSONArray criterionArray = new JSONArray();
        JSONObject criterion = new JSONObject();
        criterion.put("attributeName", "location");
        criterion.put("operator", "=");
        //s3 location
        criterion.put("attributeValue", loc);

        criterionArray.put(criterion);
        entityFilters.put("criterion", criterionArray);

        req.put("entityFilters", entityFilters);
        req.put("skipAttrValidation", false);
        //send s3 storage location to MDR
        try{
            URL url = new URL("https://metadataregistry-e2e.api.intuit.com/v2/dataregistry/entities/search/basic");
            logger.info(String.format("GET request %s for s3 to hive mapping",url));
            try{
                HttpURLConnection conn= (HttpURLConnection) url.openConnection();
                String token= "Intuit_IAM_Authentication intuit_appid=Intuit.data.dataportal.dmrofflinejob,intuit_app_secret=preprdueL2Yk3cBEcw5LDxzN5JvrXkYpNAT99Fd7 ,intuit_userid= 123148492422819";
                conn.setRequestProperty("Authorization",token);
                conn.setRequestProperty("Content-Type","application/json");
                conn.setRequestMethod("POST");
                conn.setDoOutput(true);
                // Write the message payload to the output stream
                DataOutputStream outputStream = new DataOutputStream(conn.getOutputStream());
                outputStream.writeBytes(req.toString());
                outputStream.flush();
                // Optionally, read the response from the server
                BufferedReader br = new BufferedReader(new InputStreamReader((conn.getInputStream())));
                StringBuilder sb = new StringBuilder();
                String output;
                while ((output = br.readLine()) != null) {
                    sb.append(output);
                }
                JSONObject res=new JSONObject(sb.toString()) ;
                logger.info(res.toString());
//                System.out.println("The output of curl to get the hive table name: "+res);
                //extract table name when request is found
                if(conn.getResponseCode()==200){
                    if(((int)res.get("totalCount"))==0){
                        logger.info("No tables found for "+loc);
                        throw new hiveMappingException(String.format("No tables found for the given location:%s",loc));
                    }
                    JSONArray dataSets=(JSONArray) res.get("datasets");
                    JSONObject ds= (JSONObject) dataSets.get(0) ;
//                  System.out.println("The dataset: "+ds);
                    String loc_matched=(String) ((JSONObject) ds.get("attributes")).get("location");
                    if (loc.equals(loc_matched.trim()))
                    {
                        String qn=(String) ((JSONObject) ds.get("attributes")).get("qualifiedName");
                        int index=qn.lastIndexOf("_storage");
                        String name=qn.substring(0,index);
                        conn.disconnect();
                        return name;
                    }
                    else{
                        logger.info("Could not find exact match of s3 location!");
                        throw new hiveMappingException("Exact Match Not Found!");
                    }
                }
                else{
                    throw new java.io.IOException();
                }
            }
            catch(java.io.IOException e){
                logger.severe(e.toString());
                throw new hiveMappingException("Error in s3 to hive mapping"+e.toString());
            }
        }catch (MalformedURLException e){
            logger.severe("Error in the URL:"+e);
            throw new hiveMappingException("Error in s3 to hive mapping"+e);
        }
    }

}
