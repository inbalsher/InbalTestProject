package MyPackage;

import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.json.simple.JSONArray;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.springframework.http.MediaType;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.nio.charset.StandardCharsets;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.text.SimpleDateFormat;
import java.util.Base64;
import java.util.Calendar;
import java.util.Locale;
import java.util.TimeZone;
import java.net.*;
import java.util.*;
import java.io.*;
import org.json.simple.JSONObject;


public class LogDataServices {


    private static final String timestamp = "";
    private static final String RFC_1123_DATE = "EEE, dd MMM yyyy HH:mm:ss z";
    private static final String CONTENT_TYPE = "Content-type";
    private static final int MAX_BYTES = 31457278; // 30MB - 2

    public static void main(String[] args){
    }

    public static JSONObject queryData(String clientId, String clientSecret, String tenantId, String workspace, String query, String timespan) throws IOException, ParseException {

        // Get the access token
        String token = getAccessToken(clientId, clientSecret, tenantId);
        if (token != null){
            // Query log
            return queryLog(token,workspace,query, timespan);
        }
        else{
            return null;
        }

    }

    private static String getAccessToken(String clientId, String clientSecret, String tenantId) throws IOException, ParseException {

        String token = "";

        // Set connection URL
        String strUrl = "https://login.microsoftonline.com/" + tenantId + "/oauth2/token";
        URL url = new URL(strUrl);
        HttpURLConnection con = (HttpURLConnection) url.openConnection();
        con.setRequestMethod("POST");
        // Set connection headers
        con.setRequestProperty(CONTENT_TYPE, "application/x-www-form-urlencoded");
        // Set connection parameters
        Map<String, String> parameters = new HashMap<>();
        parameters.put("grant_type", "client_credentials");
        parameters.put("client_id", clientId);
        parameters.put("client_secret", clientSecret);
        parameters.put("resource", "https://api.loganalytics.io");
        con.setDoOutput(true);
        DataOutputStream out = new DataOutputStream(con.getOutputStream());
        out.writeBytes(getParamsString(parameters));
        out.flush();
        out.close();

        // Get response status
        int status = con.getResponseCode();

        if (status != 200){

            return null;
            // throws exception with the error text from the json response
        }
        else {
            // Get the token from the response json data
            BufferedReader in = new BufferedReader(
                    new InputStreamReader(con.getInputStream()));
            String inputLine;

            while ((inputLine = in.readLine()) != null) {
                System.out.println("Received " + inputLine);
                JSONParser parser = new JSONParser();
                JSONObject responseJson = (JSONObject) parser.parse(inputLine);
                Object objToken = responseJson.get("access_token");
                token = (String)objToken;
            }

            in.close();
        }

        con.disconnect();
        return token;
    }

    private static JSONObject queryLog(String accessToken, String workspaces, String query, String timespan) throws IOException, ParseException {

        JSONObject responseJson = new JSONObject();

        // Set connection URL
        String strUrl = "https://api.loganalytics.io/v1/workspaces/" + workspaces + "/query";
        URL url = new URL(strUrl);
        HttpURLConnection con = (HttpURLConnection) url.openConnection();
        con.setRequestMethod("POST");
        // Set connection headers
        String authorization = "Bearer " + accessToken;
        con.setRequestProperty("Authorization", authorization);
        con.setRequestProperty(CONTENT_TYPE, "application/json");
        con.setDoOutput(true);

        // Set request body
        JSONObject jsonInputString = new JSONObject();
        jsonInputString.put("query", query);
        jsonInputString.put("timespan", timespan);

        try(OutputStream os = con.getOutputStream()) {
            byte[] input = jsonInputString.toString().getBytes(StandardCharsets.UTF_8);
            os.write(input, 0, input.length);
        }

        // Get response status
        int status = con.getResponseCode();

        System.out.println("Query status: " + status );

        if (status != 200){
            return null;
            // throws exception with the error text from the json response
        }
        else {
            // Get the token from the response json data

            BufferedReader in = new BufferedReader(
                    new InputStreamReader(con.getInputStream()));
            String inputLine;

            while ((inputLine = in.readLine()) != null) {
                System.out.println("Received " + inputLine);
                JSONParser parser = new JSONParser();
                responseJson = (JSONObject) parser.parse(inputLine);
              }

            in.close();
        }
        con.disconnect();
        return responseJson;
    }

    private static String getParamsString( Map<String, String> params ) {
        StringBuilder result = new StringBuilder();

        for (Map.Entry<String, String> entry : params.entrySet()) {
            result.append(URLEncoder.encode(entry.getKey(),StandardCharsets.UTF_8));
            result.append("=");
            result.append(URLEncoder.encode(entry.getValue(),StandardCharsets.UTF_8));
            result.append("&");
        }

        String resultString = result.toString();
        return resultString.length() > 0
                ? resultString.substring(0, resultString.length() - 1)
                : resultString;
    }

    public static void writeFileToLog(String fileName, String sharedKey, String workspaceId, String logName){
        try {
            // Read JSON file
            JSONArray completeFile = readJsonFromFile(fileName);
            // Post json file in bulks
            if (completeFile != null) {
                sendSplitFile(completeFile, sharedKey, workspaceId, logName);
            }
            else{
                // Empty file
            }
        }
        catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        catch (IOException e) {
            e.printStackTrace();
        }
        catch (InvalidKeyException e) {
            e.printStackTrace();
        }
    }


    private static void sendSplitFile(JSONArray file, String sharedKey, String workspaceId, String logName) throws NoSuchAlgorithmException, IOException, InvalidKeyException {
        // Split the JSON file, Maximum of 30 MB per post
        /// What if the one JSON row is to big? need to handle it while creating the file
        /// Need to check that limit for field values and maximum number of fields
        String json = "";
        String jsonTmp;
        int byteCount = 0;

        // Go over the JSON array, and create new JSON
        for (int i = 0; i < file.size(); i++) {
            if ( byteCount == 0 ){
                json = file.get(i).toString();
                byteCount = json.getBytes().length;
            }
            else {
                jsonTmp = json + "," + file.get(i).toString();

                // Check tat the JSON wont be to large for the POST request
                if ( jsonTmp.getBytes().length > MAX_BYTES ) {
                    // Post JSON data
                    json = "[" + json + "]";
                    postJsonData(json, sharedKey, workspaceId, logName);
                    // Init data
                    byteCount = 0;
                }
                else{
                    json = jsonTmp;
                    byteCount = json.getBytes().length;
                }
            }
        }

        if ( !json.isEmpty() ){
            json = "[" + json + "]";
            postJsonData(json, sharedKey, workspaceId, logName);
        }
    }

    private static void postJsonData(String jsonData, String sharedKey, String workspaceId, String logName) throws NoSuchAlgorithmException, InvalidKeyException, IOException {
        // Build the log data with authorization keys for the POST request
        String dateString = getServerTime();
        String httpMethod = "POST";
        String contentType = "application/json";
        String xmsDate = "x-ms-date:" + dateString;
        String resource = "/api/logs";

        String jsonLength = String.valueOf(jsonData.getBytes(StandardCharsets.UTF_8).length);
        String stringToHash = String.join("\n", httpMethod, jsonLength, contentType, xmsDate , resource);
        String hashedString = getHMAC256(stringToHash, sharedKey);
        String signature = "SharedKey " + workspaceId + ":" + hashedString;
        int statusCode = postData(signature, dateString, jsonData,workspaceId, logName);
        System.out.println("Status code: " + statusCode);

        if ( statusCode != 200 ){
            System.out.println("Error occurred");
            // handle sending error
            // What to do with the data that was not sent, to continue with the file upload or stop the process...
        }
    }

    private static String getServerTime() {
        // Get the server time with the relevant format
        Calendar calendar = Calendar.getInstance();
        SimpleDateFormat dateFormat = new SimpleDateFormat(RFC_1123_DATE, Locale.ENGLISH); // My locale is hebrew, has to be english

        dateFormat.setTimeZone(TimeZone.getTimeZone("GMT"));
        return dateFormat.format(calendar.getTime());
    }

    private static int postData(String signature, String dateString, String json, String workspaceId, String logName) throws IOException {
        String url = "https://" + workspaceId + ".ods.opinsights.azure.com/api/logs?api-version=2016-04-01";
        HttpPost httpPost = new HttpPost(url);
        httpPost.setHeader("Authorization", signature);
        httpPost.setHeader(CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE);
        httpPost.setHeader("Log-Type", logName);
        httpPost.setHeader("x-ms-date", dateString);
        httpPost.setHeader("time-generated-field", timestamp);

        httpPost.setEntity(new StringEntity(json));
        try(CloseableHttpClient httpClient = HttpClients.createDefault()){
            HttpResponse response = httpClient.execute(httpPost);
            int statusCode = response.getStatusLine().getStatusCode();
            return statusCode;

        }
    }

    private static String getHMAC256(String input, String key) throws InvalidKeyException, NoSuchAlgorithmException {
        String hash;
        Mac sha256HMAC = Mac.getInstance("HmacSHA256");
        Base64.Decoder decoder = Base64.getDecoder();
        SecretKeySpec secretKey = new SecretKeySpec(decoder.decode(key.getBytes(StandardCharsets.UTF_8)), "HmacSHA256");
        sha256HMAC.init(secretKey);
        Base64.Encoder encoder = Base64.getEncoder();
        hash = new String(encoder.encode(sha256HMAC.doFinal(input.getBytes(StandardCharsets.UTF_8))));
        return hash;
    }

    private static JSONArray readJsonFromFile(String fileName){
        //JSON parser object to parse read file
        JSONParser jsonParser = new JSONParser();

        try (FileReader reader = new FileReader(fileName))
        {
            //Read JSON file
            Object obj = jsonParser.parse(reader);
            return (JSONArray)obj;

        } catch (FileNotFoundException e) {
            e.printStackTrace();
            return null;
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        } catch (ParseException e) {
            e.printStackTrace();
            return null;
        }
    }

}
