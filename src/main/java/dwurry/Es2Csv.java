package dwurry;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.Map;

import org.elasticsearch.action.count.CountResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.joda.time.DateTime;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

public class Es2Csv {

    public static void main(String[] args) {

        String cluster      = null;
        String host         = null;
        String indexName    = null;
        String templateFile = null;
        String outputFile   = null;
        String performanceFile=null;
        int start           = 0;
        int volume          = 2000;
        Boolean append      = false;
        int endQueryAt    = 0;

        for (String s : args) {
            if (s.charAt(1) == 'c') {
                cluster = s.substring(3);
            } else if (s.charAt(1) == 'h'){
                host = s.substring(3);
            }else if (s.charAt(1) == 'i') {
                indexName = s.substring(3);
            }else if (s.charAt(1) == 't') {
                templateFile = s.substring(3);
            }else if (s.charAt(1) == 'o') {
                outputFile = s.substring(3);
            }else if (s.charAt(1) == 's') {
                start = new Integer(s.substring(3));
            }else if (s.charAt(1) == 'v') {
                volume = new Integer(s.substring(3));
            }else if (s.charAt(1) == 'a') {
                append = new Boolean(s.substring(3));
            }else if (s.charAt(1) == 'p') {
                performanceFile = s.substring(3);
            }else if (s.charAt(1) == 'e') {
                endQueryAt = new Integer(s.substring(3));
            }
        }

        if (cluster == null | host == null | indexName == null){
            printHelpMessage();
        }


        Es2Csv json2Csv = new Es2Csv();

        String documentType = "logs";

        BufferedWriter perfWriter = json2Csv.writePerformanceHeader(performanceFile, outputFile, append);
        BufferedWriter outputWriter = json2Csv.openFile(outputFile, append);
        ArrayList<String> headerList = json2Csv.writeCSVHeader(json2Csv.readTemplateFile(templateFile), outputWriter);
        json2Csv.scrollSearch(cluster, host, indexName, documentType, headerList, outputWriter,
                               perfWriter, start, volume, endQueryAt);
//        json2Csv.retrieveIndex(cluster, host, indexName, documentType, headerList, outputWriter,
//                               perfWriter, start, volume, endQueryAt);
        json2Csv.closeFile(outputWriter);
    }

    public static void printHelpMessage(){
        System.out.println("usage:  java -jar Es2Csv-1.0-SNAPSHOT-jar-with-dependencies.jar ");
        System.out.println("[-c=<cluster>] [-h=<host>] [-i=<index>] [-t=<template file>] [-o=<output file>] " +
                           "[-s=<start position in query>] [-v=<volume of individal requests>] "+
                           "[-a=<append (true) or overwrite (false-default) output file>]" +
                           "[-e=<There's a bug in ES that some queries don't terminate.  This stops at a record #>]");
        System.out.println("Example:  java -jar Es2Csv-1.0-SNAPSHOT-jar-with-dependencies.jar -c=esava-cluster "+
                           "-h=<host URL> -i=<ES Index> -o=20151006.cvs -s=0 "+
                           "-v=10000 -a=false");
        System.out.println("A template file is a json file to map JSON records to CSV of the format:  \n" +
                " {\n" +
                "  \"collection\": \"events\", \n" +
                "  \"map\": [\n" +
                "    [\n" +
                "      <JSON field name>, \n" +
                "      <CSV field name> \n" +
                "    ] [, ...] \n" +
                "  ]\n" +
                " }");
        System.exit(0);
    }

    //make this recursive so that it gets the whole thing...
    public void retrieveIndex(String cluster, String host, String index, String type, ArrayList<String> headerList,
                              BufferedWriter outputWriter, BufferedWriter perfWriter, int start, int volume,
                              int endQueryAt) {

        int from = 0;
        int size = volume;
        Client client = getClient(cluster, host);
        final String documentType = "logs";

        from = start;

        long retrieveSize = (endQueryAt == 0)?getIndexCount(client, index, type):endQueryAt;

        int resetClient = 0;

        while (from == 0 | retrieveSize > 0) {
//            retrieveSize = getIndexCount(client, index, type);
            try {
                String startTime = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(new Date());
                perfWriter.write(startTime + ", ");
                if(endQueryAt != 0 && from + size >= endQueryAt){
                    size = endQueryAt - from;
                }
                retrieveSize = retrieveIndex(client, index, type, from, size, headerList, outputWriter, perfWriter);
                from += retrieveSize;
                resetClient += retrieveSize;
                String endTime = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(new Date());
                perfWriter.write(from + ", " + size + ", " + retrieveSize + "," + endTime + "\n");
                perfWriter.flush();

                if (from == endQueryAt) retrieveSize = -1;  //terminates the query
            }catch (IOException e){
                System.out.println("error writing performance file");
            }
        }
    }

    public long getIndexCount(Client client, String index, String type) {
        CountResponse response = client.prepareCount(index)
                .setTypes(type)
                .setQuery(QueryBuilders.matchAllQuery())
                .execute()
                .actionGet();

        long count = response.getCount();

        return count;
    }


    /**
     *
     * @param index
     * @param type
     * @param from
     * @param size
     * @param headerList
     * @param outputWriter
     * @return number of rows queried
     */
    public int retrieveIndex(Client client, String index, String type, int from, int size,
                             ArrayList<String> headerList, BufferedWriter outputWriter,
                             BufferedWriter perfWriter) {

        //Client client = getClient(cluster, host);

        SearchResponse response = client.prepareSearch(index)
                .setTypes(type)
//                .setSearchType(SearchType.SCAN)
//                .setScroll(new TimeValue(60000)) //TimeValue?
                .setSearchType(SearchType.QUERY_THEN_FETCH)
                .setQuery(QueryBuilders.matchAllQuery())
                .setFrom(from).setSize(size).setExplain(true)
                .execute()
                .actionGet();

        SearchHit[] results = response.getHits().getHits();
        try {
            perfWriter.write(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(new Date()) + ",");
        } catch (IOException e){
            System.out.println("Error writing endQueryTS to log performance file");
            e.printStackTrace();
        }
        for (SearchHit hit : results) {
            Map<String,Object> result = hit.getSource();
            this.write2Csv(result, headerList, outputWriter);
            //this.writeMap(result);  // debugging
        }

        return results.length;

    }

    public void scrollSearch(String cluster, String host, String index, String type, ArrayList<String> headerList,
                            BufferedWriter outputWriter, BufferedWriter perfWriter, int start, int volume,
                            int endQueryAt) {
        int from = start;
        DateTime startDate = new DateTime(DateTime.now());
        Client client = getClient(cluster, host);

        String startTime = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(new Date());
        SearchResponse scrollResp = client.prepareSearch(index)
                .setSearchType(SearchType.SCAN)
                .setScroll(new TimeValue(60000))
                .setQuery(QueryBuilders.matchAllQuery())
                .setSize(volume)
                .execute().actionGet();
        String endTime = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(new Date());
        try {
            perfWriter.write(startTime + ", " + endTime + "," + from + "," + volume + "," + 0 + "," +
                    new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(new Date()) + "\n");
        } catch (IOException e){
            System.out.println("Error writing out initial performance record");
        }
        int recsReturned = 0;
        while (true) {
            recsReturned = page(client, scrollResp, headerList, outputWriter, perfWriter,start, startTime, volume, from);
            from += recsReturned;
            if (recsReturned == 0){
                DateTime endDate = new DateTime(DateTime.now());
                long elapsedMS = endDate.getMillis() - startDate.getMillis();
                int elapsedS = (int) elapsedMS/1000;
                int recsPerSecond = from/elapsedS;
                System.out.println("Query Completed:  " + from + " records in " + elapsedS + " =" + from/elapsedS +
                        "/recsPerSecond");
                System.out.println("Batch size = " + volume + "/" + volume*5);
                break;
            }
        }
    }

    private int page(Client client, SearchResponse scrollResp, ArrayList<String> headerList,
                      BufferedWriter outputWriter, BufferedWriter perfWriter, int start, String startQuery, int volume,
                      int from) {

        int recsRetunred=0;
        try {
            startQuery = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(new Date());
            perfWriter.write(startQuery + ", ");
            scrollResp = client.prepareSearchScroll(scrollResp.getScrollId()).
                    setScroll(new TimeValue(60000)).
                    execute().actionGet();
            String endQuery = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(new Date());
            recsRetunred = scrollResp.getHits().getHits().length;
            for (SearchHit hit : scrollResp.getHits().getHits()) {
                Map<String, Object> result = hit.getSource();
                this.write2Csv(result, headerList, outputWriter);
            }
            perfWriter.write(endQuery + ", ");
            String writeTime = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(new Date());
            perfWriter.write(from + ", " + volume + ", " + recsRetunred + "," + writeTime + "\n");
            perfWriter.flush();
        } catch(IOException e){
            System.out.println("Error writing performance file");
            e.printStackTrace();
        }
        //Break condition: No hits are returned
        return recsRetunred;
    }



    private static TransportClient getClient(String cluster, String host) {

        String[] hostNames = new String[]{host};
        hostNames[0] = host;

        Settings settings =
                ImmutableSettings.settingsBuilder().put("cluster.name", cluster).build();
        TransportClient client = new TransportClient(settings);

        for (String esHostName : hostNames) {
            client = client.addTransportAddress(new InetSocketTransportAddress(esHostName, 9300));
        }

        return client;
    }


    /**
     * Opens the template file and returns the map of Json fields to CSV fields.  The list returned is in CSV header file order.
     * @param filename
     * @return an Iterator of JSON objects [<JSON Field NAME>, <CSV Field Name>] ,...
     */
    public Iterator<JSONArray> readTemplateFile(String filename){
        JSONParser parser = new JSONParser();
        Iterator<JSONArray> iterator = null;

        try {

            Object obj = parser.parse(new FileReader(filename));

            JSONObject jsonObject = (JSONObject) obj;

            JSONArray fieldList = (JSONArray) jsonObject.get("map");

            iterator = fieldList.iterator();

        }catch (FileNotFoundException e){
            System.out.println("The file: "+filename + " was not found on the file system.  Be sure the file exists and is readable from this process");
            e.printStackTrace();
            System.exit(0);
        } catch (IOException e) {
            System.out.println("The file: " + filename + " had an IO exception...probably does not have proper JSON objects");
            e.printStackTrace();
        } catch (ParseException e) {
            System.out.println("The file: " + filename + " had a parse exception...probably does not have proper JSON objects");
            e.printStackTrace();
        }

        return iterator;

    }

    public BufferedWriter writePerformanceHeader(String performanceFile, String outputFile, boolean append){
        BufferedWriter outputWriter = null;
        performanceFile = (performanceFile == null)?"Perf" + outputFile : performanceFile;

        try {
            File file = new File(performanceFile);
            if (!file.exists()) {  //create if does not exist
                file.createNewFile();
            }

            FileWriter fw = new FileWriter(file.getAbsoluteFile(), append);
            outputWriter = new BufferedWriter(fw);
            //csv file header
            outputWriter.write("startTS, endQueryTS, retrieved, request, returned, endWriteTS\n");
        } catch (IOException e) {
            System.out.println("Output file failed to open... " + performanceFile);
            this.printHelpMessage();
            e.printStackTrace();
        } catch (NullPointerException e){
            System.out.println("Output file failed to open... " + performanceFile);
            this.printHelpMessage();
            e.printStackTrace();
        }

        return outputWriter;


    }

    public ArrayList<String> writeCSVHeader(Iterator<JSONArray> templateRecords, BufferedWriter outputWriter){
        ArrayList<String> jsonFields = new ArrayList<String>();

        try {
            ArrayList<String> nextHeader = templateRecords.next();
            outputWriter.write(nextHeader.get(1));
            jsonFields.add(nextHeader.get(0));
            while (templateRecords.hasNext()) {
                nextHeader = templateRecords.next();
                outputWriter.write("," + nextHeader.get(1));
                jsonFields.add(nextHeader.get(0));
            }
            outputWriter.write("\n");
        } catch (IOException e){
            System.out.println("Write to file " + outputWriter.toString() + " (outputFile) failed");
            e.printStackTrace();
        }

        return jsonFields;
    }

    public BufferedWriter openFile(String outputFile, boolean append) {
        BufferedWriter outputWriter = null;

        try {
            File file = new File(outputFile);
            if (!file.exists()) {  //create if does not exist
                file.createNewFile();
            }

            FileWriter fw = new FileWriter(file.getAbsoluteFile(), append);
            outputWriter = new BufferedWriter(fw);
        } catch (IOException e) {
            System.out.println("Output file failed to open... " + outputFile);
            this.printHelpMessage();
            e.printStackTrace();
        } catch (NullPointerException e){
            System.out.println("Output file failed to open... " + outputFile);
            this.printHelpMessage();
            e.printStackTrace();
        }
        return outputWriter;
    }

    public void closeFile(BufferedWriter bw){
        try {
            bw.close();
        } catch (IOException e){
            System.out.println("Output file failed to close... " + bw.toString());
            e.printStackTrace();
        }
    }

    public void write2Csv(Map<String, Object> resultMap, ArrayList<String> jsonFields, BufferedWriter outputWriter){

        Map<String, Object> columnMap = resultMap;
        try{
///*debug*/   int commaCount = 0;
            outputWriter.write((String) columnMap.get(jsonFields.get(0)));
            for(int i = 1; i < jsonFields.size(); i++){  //this is gona happen billions of times...be efficient here!
                columnMap = resultMap;
                String fieldName = jsonFields.get(i);
                String field;
                if (fieldName.contains(".")) {
                    String parent = fieldName.substring(0, fieldName.indexOf('.'));
                    fieldName = fieldName.substring(fieldName.indexOf('.') + 1);
//                    writeMap(columnMap);
//                    System.out.println("parent: " + parent + " field: " + fieldName);
                    columnMap = (Map<String, Object>) columnMap.get(parent);
                }
                Object temp = (columnMap == null)?null:columnMap.get(fieldName);
                field = (temp == null)?null:temp.toString();
                outputWriter.write("," + ((field == "null" | field == null)?"":"\""+field+"\""));
//debug
//                commaCount += 1;
//                if (fieldName.equalsIgnoreCase("sw_plat")) System.out.println("column: " + (commaCount) + " name: " + fieldName);
//                if (fieldName.equalsIgnoreCase("sw_plat") && field == "3499"){
//                    System.out.println("Got an odd ball.  Field:" + fieldName + " value: " + field +
//                                       " position:" + (commaCount));
//                }
//                if (field != null && field.contains(","))System.out.println("Got a field with a comma!  " + "Field:" + fieldName + " value: " + field +
//                                                            " position:" + commaCount);
//debug (end)
            }
            outputWriter.write("\n");
///* debug */ if (commaCount !=309) System.out.println("Fields in row: " + (commaCount +1));
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public void writeMap(Map<String, Object> map){
        System.out.println("\n\n\n______________ NEW MAP _________________\n");
        for (Map.Entry<String, Object> entry : map.entrySet())
        {
            System.out.println(entry.getKey() + "/" + entry.getValue());
        }
    }
}