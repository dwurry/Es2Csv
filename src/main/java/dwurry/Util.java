package dwurry;

import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

/**
 * Created by davidurry on 11/3/15.
 */
public class Util {
    protected static TransportClient getClient(String cluster, String host) {

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

}
