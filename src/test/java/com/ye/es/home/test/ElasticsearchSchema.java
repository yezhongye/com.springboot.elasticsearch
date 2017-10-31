package com.ye.es.home.test;

import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.client.Client;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by allwefantasy on 8/26/16.
 */
public class ElasticsearchSchema extends AbstractSchema {
    final String index;

    private transient Client client;

    /**
     * Creates an Elasticsearch schema.
     *
     * @param coordinates Map of Elasticsearch node locations (host, port)
     * @param userConfig  Map of user-specified configurations
     * @param indexName   Elasticsearch database name, e.g. "usa".
     */
    public ElasticsearchSchema(Map<String, Integer> coordinates,
                               Map<String, String> userConfig, String indexName) {
        super();

        final List<InetSocketAddress> transportAddresses = new ArrayList<>();
        for (Map.Entry<String, Integer> coordinate : coordinates.entrySet()) {
            transportAddresses.add(new InetSocketAddress(coordinate.getKey(), coordinate.getValue()));
        }

        open(transportAddresses, userConfig);

        if (client != null) {
            final String[] indices = client.admin().indices()
                    .getIndex(new GetIndexRequest().indices(indexName))
                    .actionGet().getIndices();
            if (indices.length == 1) {
                index = indices[0];
            } else {
                index = null;
            }
        } else {
            index = null;
        }
    }

    @Override
    protected Map<String, Table> getTableMap() {
//        final ImmutableMap.Builder<String, Table> builder = ImmutableMap.builder();
//
//        try {
//            GetMappingsResponse response = client.admin().indices().getMappings(
//                    new GetMappingsRequest().indices(index)).get();
//            ImmutableOpenMap<String, MappingMetaData> mapping = response.getMappings().get(index);
//            for (ObjectObjectCursor<String, MappingMetaData> c : mapping) {
//                builder.put(index + "/" + c.key, new ElasticsearchTable(client, index, c.key, c.value));
//            }
//            builder.put(index, new ElasticsearchTable(client, index, null, mapping.valuesIt().next()));
//        } catch (Exception e) {
//            throw Throwables.propagate(e);
//        }
//        return builder.build();
        return null;
    }

    private void open(List<InetSocketAddress> transportAddresses, Map<String, String> userConfig) {
//        final List<TransportAddress> transportNodes = new ArrayList<>(transportAddresses.size());
//        for (InetSocketAddress address : transportAddresses) {
//            transportNodes.add(new InetSocketTransportAddress(address));
//        }
//
//        Settings settings = Settings.settingsBuilder().put(userConfig).put("client.transport.ignore_cluster_name", true).build();
//
//        final TransportClient transportClient = TransportClient.builder().settings(settings).build();
//        for (TransportAddress transport : transportNodes) {
//            transportClient.addTransportAddress(transport);
//        }
//
//        final List<DiscoveryNode> nodes = ImmutableList.copyOf(transportClient.connectedNodes());
//        if (nodes.isEmpty()) {
//            throw new RuntimeException("Cannot connect to any elasticsearch nodes");
//        }
//
//        client = transportClient;
//    }
    }
}