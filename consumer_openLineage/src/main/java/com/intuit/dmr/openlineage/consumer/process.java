package com.intuit.dmr.openlineage.consumer;
import java.util.Iterator;
//import org.json.JSONArray;
//import org.json.JSONObject;
import org.json.simple.*;

import java.util.logging.Logger;
//logging done

public class process {
    //1. Process the message; check if it has inputs and outputs
    //2. Create a payload
    public static final Logger logger= java.util.logging.Logger.getLogger(process.class.getName());

   public static boolean isValid(JSONObject event){
       logInit.initializeLogger();
       logger.finer("Checking if the event is valid for further processing");
       JSONArray inputs = (JSONArray) event.get("inputs");
       JSONArray outputs = (JSONArray) event.get("outputs");
       String status=(String) event.get("eventType");
       if (inputs.isEmpty() || outputs.isEmpty() || (!status.contains("COMPLETE")))
       {
           return false;
       }
       return true;
   }

    //Parsing a json file
    public static JSONObject createPayLoad(JSONObject event) throws hiveMappingException
   {   logInit.initializeLogger();
       //inputs in the event received
       logger.info("Inside the create payload function");

       JSONArray inputs = (JSONArray) event.get("inputs");
       JSONArray outputs = (JSONArray) event.get("outputs");
       logger.info("Creating the payload");
       JSONObject job=(JSONObject)event.get("job");
       logger.info(job.toString());
       String runInfo=(String) job.get("namespace");
       logger.info("Job details"+runInfo);
       String uid="";
       //input array of input json objects
       JSONArray inArray=new JSONArray();
       //output array of output json objects
       JSONArray outArray=new JSONArray();
       Iterator iitr = inputs.iterator();
       Iterator oitr = outputs.iterator();
       while (iitr.hasNext())
       {
           JSONObject input=new JSONObject();
           JSONObject entry=(JSONObject) iitr.next();
           String name=(String)entry.get("name");
           uid+=name+"_";
           String namespace=(String)entry.get("namespace");
           //full name of the input
           String fname="";
           if ((namespace.charAt(namespace.length()-1)!='/') && (name.charAt(0)!='/')){
               //combining namespace and file name to get the final name of the file
               fname=namespace+"/"+name;
           }
           else{
               fname=namespace+""+name;
           }
           logger.info("The input is:"+fname);
           //check if namespace is an s3 location
           if (namespace.contains("s3"))
           {
               logger.info(String.format("%s is an s3 location",fname));
//               System.out.println("s3 to hive mapping of- "+fname);
               //properly catch exceptions here
               name= searchHive.s3ToHive(fname);
           }
           input.put("urn",name);
           input.put("type","hive_table");
//           inArray.add(input);
           inArray.add(input);
//           System.out.println(input);
       }
       while (oitr.hasNext())
       {
           JSONObject output=new JSONObject();
           JSONObject entry=(JSONObject) oitr.next();
           String name=(String)entry.get("name");
           uid+=name+"_";
           String namespace=(String)entry.get("namespace");
           String fname=name+namespace;
           if (namespace.contains("s3")){
               System.out.println("s3 to hive mapping of- "+fname);
               //throw hiveMappingException back
               name= searchHive.s3ToHive(fname);
           }
           output.put("urn",name);
           output.put("type","hive_table");
           outArray.add(output);
       }
       uid=uid.substring(0,uid.length()-1);
       JSONObject payload=new JSONObject();
       payload.put("type","processor");
       payload.put("urn",uid);
       JSONObject attributes=new JSONObject();
       attributes.put("name",uid);
       attributes.put("status","NEW");
       attributes.put("id",uid);
       attributes.put("accessGroupId","test");
       payload.put("attributes",attributes);
       payload.put("classifications",new JSONArray());
       payload.put("relationships",new JSONArray());
       payload.put("inputs",inArray);
       payload.put("outputs",outArray);
       return payload;
   }
}

