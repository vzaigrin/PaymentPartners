package pps.ppapp;

import com.google.cloud.bigquery.*;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;

public class Model {
    private static BigQuery bigquery;

    private static Model instance;

    private Model() throws IOException {
        // Load credentials from JSON key file.
        File credentialsPath = new File("service_account.json");
        try (FileInputStream serviceAccountStream = new FileInputStream(credentialsPath)) {
            GoogleCredentials credentials = ServiceAccountCredentials.fromStream(serviceAccountStream);
            bigquery = BigQueryOptions.newBuilder().setCredentials(credentials).build().getService();
        }
    }

    public static Model getInstance() throws IOException {
        if (instance == null) {
            instance = new Model();
        }
        return instance;
    }

    // Список отчётов
    private static final HashMap<String, String> reports = new HashMap<String, String>() {{
        put("Полный", "DM_REPORT");
        put("Карты", "V_DM_CARD");
        put("Банки", "V_DM_BANK");
        put("Города", "V_DM_CITY");
        put("Привилегии", "V_DM_PRIVILEGE");
        put("Средний чек", "V_DM_PAYMENT");
    }};

    public HashMap<String, String> getReports() {
        return reports;
    }

    private TableResult request(String query) throws Exception {
        // Create the query job.
        QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(query).setUseLegacySql(false).build();

        // Create a job ID so that we can safely retry.
        JobId jobId = JobId.of(UUID.randomUUID().toString());
        Job queryJob = bigquery.create(JobInfo.newBuilder(queryConfig).setJobId(jobId).build());

        // Wait for the query to complete.
        queryJob = queryJob.waitFor();

        // Check for errors
        if (queryJob == null) {
            throw new RuntimeException("Job no longer exists");
        } else if (queryJob.getStatus().getError() != null) {
            throw new RuntimeException(queryJob.getStatus().getError().toString());
        }

        // Get the results.
        return queryJob.getQueryResults();
    }

    public Boolean validateUser(String username, String password) throws Exception {
        TableResult tableResult = request("DECLARE result STRING;\n" +
                "CALL PP.VALIDATE_USER ('" + username + "', '" + password + "', result);\n" +
                "SELECT result;");
        String result = tableResult.iterateAll().iterator().next().get("result").getValue().toString();
        return result.equals("Valid");
    }

    public String getUserRole(String username) throws Exception {
        TableResult tableResult = request("SELECT role FROM PP.U_USERS WHERE USERNAME = '" + username + "';");
        return tableResult.iterateAll().iterator().next().get("role").getValue().toString();
    }

    public String addSession(String username) throws Exception {
        TableResult tableResult = request("DECLARE result STRING;\n" +
                "CALL PP.ADD_SESSION ('" + username + "', result);\n" +
                "SELECT result;");
        return tableResult.iterateAll().iterator().next().get("result").getValue().toString();
    }

    public Boolean validateSession(String sid) throws Exception {
        TableResult tableResult = request("DECLARE result STRING;\n" +
                "CALL PP.VALIDATE_SESSION ('" + sid + "', result);\n" +
                "SELECT result;");
        String result = tableResult.iterateAll().iterator().next().get("result").getValue().toString();

        return result.equals("Valid");
    }

    public void closeSession(String sid) throws Exception {
        request("CALL PP.CLOSE_SESSION ('" + sid + "');");
    }

    public Report getReport(String name, String username, String role) throws Exception {
        Report report = new Report();
        String query;
        if (role.equals("user"))
            query = "SELECT * EXCEPT (partner_name) FROM PP." + name + " WHERE partner_name = '" + username + "';";
        else
            query = "SELECT * FROM PP." + name + ";";

        TableResult tableResult = request(query);

        // Заголовок
        List<String> head = new ArrayList<>();
        for (Field field : tableResult.getSchema().getFields()) {
            head.add(field.getName());
        }
        report.setHead(head);

        // Данные
        List<List<String>> data = new ArrayList<>();
        for (FieldValueList row : tableResult.iterateAll()) {
            List<String> r = new ArrayList<>();
            for (FieldValue field : row) {
                r.add(field.getValue().toString());
            }
            data.add(r);
        }
        report.setData(data);

        return report;
    }
}
