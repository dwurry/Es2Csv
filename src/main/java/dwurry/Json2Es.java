package dwurry;

import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

/**
 * Created by davidurry on 10/6/15.
 */
public class Json2Es {
    public static void main(String[] args) {

        String cluster      = null;
        String host         = null;
        String indexName    = null;
        String docName      = null;
        String inputFile    = null;
        TransportClient client  = null;


        for (String s : args) {
            if (s.charAt(1) == 'c') {
                cluster = s.substring(3);
            } else if (s.charAt(1) == 'h'){
                host = s.substring(3);
            }else if (s.charAt(1) == 'i') {
                indexName = s.substring(3);
            }else if (s.charAt(1) == 'd') {
                docName = s.substring(3);
            }else if (s.charAt(1) == 'f') {
                inputFile = s.substring(3);
            }
        }

        if (cluster == null | host == null | indexName == null | docName == null | inputFile == null){
            printHelpMessage();
        }
        client = Util.getClient(cluster, host);
        new Json2Es().readJson(client, indexName, docName, inputFile);
    }

    public static void printHelpMessage(){
        System.out.println("usage:  java -cp Es2Csv-1.0-SNAPSHOT-jar-with-dependencies.jar Json2Es");
        System.out.println("[-c=<cluster>] [-h=<host>] [-i=<index>] [-d=<document>] [-f=<input file>] ");
        System.out.println("Example:  java -cp Es2Csv-1.0-SNAPSHOT-jar-with-dependencies.jar -c=esava-cluster "+
                "-h=<host URL> -i=<ES Index> -d=<ES Document Type> -f=my.json");

        System.exit(0);
    }

    public void readJson(TransportClient client, String indexName, String docName, String inputFile){

        String line = null;
        int lineCount = 1;

        try {
            BufferedReader br = new BufferedReader(new FileReader(inputFile));
            while ((line = br.readLine()) != null) {
                if (line.length()>5) {
                    JSONObject json = (JSONObject) new JSONParser().parse(line);
                    IndexResponse resp = indexJson(client, indexName, docName, json);
                    lineCount++;
                }
            }
        }catch(FileNotFoundException e){
            System.out.println("The file " + inputFile + " could not be found. ");
            e.printStackTrace();
        } catch (IOException e){
            System.out.println("an IO excepton was generated while reading " + inputFile +".");
            e.printStackTrace();
        } catch (ParseException e){
            System.out.println("Failed to parse the record from " + inputFile +"." + "\n" +
                "The record is:  \n " + line);
            e.printStackTrace();
        }
    }

    public static IndexResponse indexJson(TransportClient client, String indexName, String docName, JSONObject json){
        IndexResponse response = client.prepareIndex(indexName, docName)
                .setSource(json)
                .execute()
                .actionGet();
        return response;
    }

}
