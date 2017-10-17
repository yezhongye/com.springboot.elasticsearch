package com.ye.es.home.test;

import com.ye.es.my.factory.EsClientFactory;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.junit.Test;

/**
 * Created by zjx on 2017/10/13 0013.
 */
public class EsClientFzQueryTest {
    Logger log = LogManager.getLogger(EsClientFzQueryTest.class);

    @Test
    public void searchTest(){
        SearchResponse response = EsClientFactory.getTransportClient().prepareSearch("indexmedicine", "twitter01")
                .setTypes("typemedicine", "tweet01")
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .setQuery(QueryBuilders.termQuery("name", "感"))
                .setQuery(QueryBuilders.termQuery("name", "冒"))
                .setQuery(QueryBuilders.termQuery("name", "灵"))
                .setQuery(QueryBuilders.termQuery("name", "颗"))
                .setQuery(QueryBuilders.termQuery("name", "粒"))
                .setPostFilter(QueryBuilders.rangeQuery("id").from(1).to(4))
                .setFrom(0).setSize(60).setExplain(true)
                .get();
        log.info(response);

//        SearchResponse response2 = EsClientFactory.getTransportClient().prepareSearch("twitter", "twitter01").get();
//        log.info("=="+response2);
    }
}
