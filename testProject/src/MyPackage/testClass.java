package MyPackage;

import org.json.simple.JSONObject;
import org.json.simple.parser.ParseException;

import java.io.IOException;

import static MyPackage.LogDataServices.*;

public class testClass {

    //private static final String workspaceId = "433098ae-45d5-4999-a98d-5cd063fbfc0d"; Inbal
    private static final String workspaceId = "6ae989e8-2e1f-4166-a559-c9073022507d";
    //private static final String sharedKey = "UcFTfvIXq2yGi1wWFHgEtE+foev7aJ5cfPMGZ2VZeu7cdaJqjVqOF9z+zQvhtkdpEcYMGkKU1fao7w3xza6Jhg=="; Inbal
    private static final String sharedKey = "rczNvU/AQOGhhJ/C8Y5cJEw6/Sv3k9sk6IEaobzb358UdgUmAAcMF6x99jlqHJnO7Oxp7foOI9hIHJv+UmTbbA==";
    private static final String logName = "InbalFileTest";
    private static final String fileName = "jsonDataFile.json";
    private static final String clientId = "da1496a7-48d1-48d0-b8fc-2287635984e5";
    private static final String clientSecret = "c.AcJiJmDypS3a1gHlU72A8G_olr-I_CLr";
    private static final String tenantId = "5b5a146c-eba8-46af-96f8-e31b50d15a3f";
    private static final String query = "InbalFileTest_CL | take 10";
    private static final String timespan = "PT2H";

    public static void main(String[] args) {



        LogDataServices.writeFileToLog(fileName, sharedKey, workspaceId, logName);
        try {
            JSONObject jsonQuery = queryData(clientId,clientSecret,tenantId,workspaceId,query,timespan);
            if (jsonQuery != null) {
                System.out.println("Log data: " + jsonQuery);
            }
        } catch (IOException | ParseException e) {
            e.printStackTrace();
        }
    }
}

