package com.intuit.dmr.openlineage.consumer;
import java.net.HttpURLConnection;
import java.net.URL;
import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.InputStreamReader;
import java.util.logging.Logger;
import org.json.simple.*;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import java.util.logging.Logger;

public class getToken {
    public static final Logger logger= java.util.logging.Logger.getLogger(getToken.class.getName());
    public static String token()
    {   logInit.initializeLogger();
        try {
            String host = "identityinternal-e2e.api.intuit.com";
            String endpoint = "/v1/graphql";

            // Create the connection
            URL url = new URL("https://" + host + endpoint);
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("POST");
            conn.setRequestProperty("Content-Type", "application/json");
            conn.setRequestProperty("Authorization","Intuit_IAM_Authentication intuit_appid=Intuit.data.dataportal.dmrcrawlerjob,intuit_app_secret=preprdRv3SBYyap4MabaUJMPDhLbCcyRXP7ybRpb");
            conn.setDoOutput(true);
            conn.setRequestProperty("intuit_tid","testMDRIntg1");

            // Set the request body
            String jsonInput = "{\"query\": \"mutation identitySignInInternalApplicationWithPrivateAuth($input: Identity_SignInApplicationWithPrivateAuthInput!) { identitySignInInternalApplicationWithPrivateAuth(input: $input) { accessToken {            token            tokenType            expiresInSeconds }        refreshToken {            token            tokenType            expiresInSeconds }        accountContext {            accountId            profileId            namespace            pseudonymId        }   authorizationHeader }}\",\"variables\": { \"input\": { \"profileId\": \"9341450829446918\" }}}";
            try (DataOutputStream outputStream = new DataOutputStream(conn.getOutputStream())) {
                outputStream.writeBytes(jsonInput);
                outputStream.flush();
            }

            // Get the response
            int responseCode = conn.getResponseCode();
//            System.out.println("Response Code: " + responseCode);

            // Read the response data
            BufferedReader reader = new BufferedReader(new InputStreamReader(conn.getInputStream()));
            StringBuilder response = new StringBuilder();
            String line;
            while ((line = reader.readLine()) != null) {
                response.append(line);
            }
            reader.close();

            // Extract the access token from the response
            String accessToken = extractAccessToken(response.toString());


            // Build the authorization header
            String prefix = "Intuit_IAM_Authentication intuit_realmid=123145964165782,intuit_token=" + accessToken + ",intuit_token_type=IAM-Ticket,intuit_userid=9130359587435826,intuit_appid=Intuit.data.dataportal.dmrcrawlerjob,intuit_app_secret=preprdRv3SBYyap4MabaUJMPDhLbCcyRXP7ybRpb";

            return prefix;
        } catch (Exception e) {
            logger.severe("Error getting Authorization Token");
            return "";
        }


    }

    public static String extractAccessToken(String response)
    {

        JSONParser parser= new JSONParser();
        try{
            JSONObject obj=(JSONObject) parser.parse(response);
//            System.out.println(obj);
            String accessToken=(String)((JSONObject)((JSONObject) ((JSONObject) obj.get("data")).get("identitySignInInternalApplicationWithPrivateAuth")).get("accessToken")).get("token");


            return accessToken;
        }
        catch (ParseException e)
        {
            logger.severe("Error getting the authorization token");
            return null;
        }

    }


}
