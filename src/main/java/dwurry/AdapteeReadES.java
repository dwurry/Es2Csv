package dwurry;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;

import java.io.*;
import java.util.HashMap;
import java.util.Map;

public class AdapteeReadES implements IAdapteeRead {

    String cluster      = "esava-cluster";
    String host         = "telemetry-es1.ava.expertcity.com";
    String indexName    = "collaboration-";
    String documentType = "logs";
    int    volume       = 1000;

    TransportClient client      = null;
    SearchResponse  scrollResp  = null;

//    IAdapteeObject  result = null;
    IAdapteeWrite   output = null;

    public void initialize(IAdapteeWrite output){
        String cluster      = "esava-cluster";
        String host         = "telemetry-es1.ava.expertcity.com";
        String indexName    = "collaboration*";
        String documentType = "logs";
        int    volume       = 1000;

        output.initialize();
        this.output = output;

        client = getClient(cluster, host);

        this.scrollResp = client.prepareSearch(indexName)
                .setSearchType(SearchType.SCAN)
                .setScroll(new TimeValue(60000))
                .setQuery(QueryBuilders.matchAllQuery())
                .setSize(volume)
                .execute().actionGet();
    }

    public void read(){
        while (true) {
            int recsReturned = page();
            if (recsReturned == 0){
                break;
            }
        }
    }

    public int page() {
            scrollResp = client.prepareSearchScroll(scrollResp.getScrollId()).
                    setScroll(new TimeValue(60000)).
                    execute().actionGet();
            int recsRetunred = scrollResp.getHits().getHits().length;
                for (SearchHit hit : scrollResp.getHits().getHits()) {
                    HashMap<String, Object>result = ((hit.getSource() instanceof HashMap) ?
                                                    (HashMap) hit.getSource() :
                                                    new HashMap<String, Object>(hit.getSource()));
                    IAdapteeObject row = new AdapteeObjectMap();
                    row.store(result);
                    output.write(row);
                }
        return recsRetunred;
    }

    public void createObject(){

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

    public void closeFile(BufferedWriter bw){
        try {
            bw.close();
        } catch (IOException e){
            System.out.println("Output file failed to close... " + bw.toString());
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