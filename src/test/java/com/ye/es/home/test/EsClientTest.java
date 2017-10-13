package com.ye.es.home.test;

import com.ye.es.my.factory.EsClientFactory;
import com.ye.es.my.util.EsUtils;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.Test;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Date;
import java.util.concurrent.ExecutionException;

/**
 * Created by zjx on 2017/10/11 0011.
 */
public class EsClientTest {
    Logger log = LogManager.getLogger(EsClientTest.class);

    public TransportClient createTTransportClient() throws UnknownHostException {
        Settings settings = Settings.builder()
                .put("cluster.name", "hb-eslog").build();
        TransportClient client = new PreBuiltTransportClient(settings)
//                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("192.168.3.220"), 9200))
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("192.168.3.220"), 9300));
        return client;
    }


    public void close(TransportClient client){
        client.close();
    }

    @Test
    public void clientTest() throws IOException {

        TransportClient client = this.createTTransportClient();
        System.out.println(client.toString());
        GetResponse response = client.prepareGet("messages-2017.09.29","logs","AV7Mp61Tl8e-24lpjaDR").get();
        System.out.println(response);
        client.close();

    }
//=================================
    @Test
    public void clientFactoryQueryTest(){
        String indexname = "messages-2017.09.29";
        String type ="logs" ;
        String id = "AV8VJMdMswldUW4R773K";
        EsUtils esUtil = new EsUtils();
        GetResponse response = esUtil.getIndexResponse(indexname, type, id);
        log.info("111:" + response);
        System.out.println(response);
    }

    @Test
    public void clientFactoryCreateTest(){
        String indexname = "messages-2017.09.29";
        String type ="logs" ;
//        String id = "AV7Mp61Tl8e-24lpjaDR";
        String json = "{" +
                "\"user\":\"kimchy12345\"," +
                "\"postDate\":\"2013-01-30\"," +
                "\"message\":\"trying out Elasticsearch 12345\"" +
                "}";
        EsUtils esUtil = new EsUtils();
        IndexResponse response = esUtil.createIndexResponse(indexname,type ,json );
        System.out.println(response);
    }
    @Test
    public void clientFactoryUpdateTest() throws ExecutionException, InterruptedException {
        String indexname = "messages-2017.09.29";
        String type ="logs" ;
        String id = "AV8VJMdMswldUW4R773K";
        String json = "trying out Elasticsearch 12345 update by script";
        EsUtils esUtil = new EsUtils();
        UpdateResponse response = esUtil.updateIndexResponseByScript(indexname, type, id, json);
        System.out.println(response);
    }
    @Test
    public void clientFactoryUpdateTest2() throws ExecutionException, InterruptedException, IOException {
        String indexname = "messages-2017.09.29";
        String type ="logs" ;
        String id = "AV8VCSSCswldUW4R7728";
        String json = "trying out Elasticsearch 12345 update by merging";
        EsUtils esUtil = new EsUtils();
        UpdateResponse response = esUtil.updateIndexResponseByMerging(indexname, type, id, json);
        System.out.println(response);
    }
    @Test
    public void clientFactoryDeleteTest(){
        String indexname = "messages-2017.09.29";
        String type ="logs" ;
        String id = "AV8VCSSCswldUW4R7728";
        EsUtils esUtil = new EsUtils();
        DeleteResponse response = esUtil.deleteIndexResponse(indexname, type, id);
        System.out.println(response);
    }
}
