package com.hgh.es_demo.controller;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hgh.es_demo.entity.EsTest;
import org.apache.commons.lang3.StringUtils;
import org.apache.lucene.search.BooleanQuery;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;
import java.util.Arrays;
import java.util.UUID;

@RestController
public class TestController {

    @Value("${es.index.my_test}")
    private String esIndex;

    @Autowired
    private RestHighLevelClient restHighLevelClient;

    /**
     * 创建索引
     * @return
     * @throws IOException
     */
    @GetMapping("/createIndex")
    public Boolean createIndex() throws IOException {
//        IndexRequest indexRequest = new IndexRequest(esIndex);
//        restHighLevelClient.index(indexRequest, RequestOptions.DEFAULT);
        CreateIndexRequest request = new CreateIndexRequest(esIndex);
        CreateIndexResponse response = restHighLevelClient.indices().create(request, RequestOptions.DEFAULT);
        System.out.println(response.toString());
        return true;
    }

    /**
     * 删除索引
     * @return
     * @throws IOException
     */
    @GetMapping("/deleteIndex")
    public Boolean deleteIndex() throws IOException {
        DeleteIndexRequest request = new DeleteIndexRequest(esIndex);
        AcknowledgedResponse response = restHighLevelClient.indices().delete(request, RequestOptions.DEFAULT);
        System.out.println(response);
        return true;
    }

    /**
     * 插入数据
     * @throws IOException
     */
    @GetMapping("/putIndex")
    public void putIndex() throws IOException {
        EsTest esTest = new EsTest();
        esTest.setId(UUID.randomUUID().toString());
        esTest.setName("张三");
        esTest.setDesc("this is test doc");
        esTest.setTags(Arrays.asList("java","大数据","elastic"));

        IndexRequest request = new IndexRequest(esIndex);
        request.id(esTest.getId());
        request.source(new ObjectMapper().writeValueAsString(esTest), XContentType.JSON);
        IndexResponse index = restHighLevelClient.index(request, RequestOptions.DEFAULT);
        System.out.println(index.status().getStatus());
        System.out.println(DocWriteResponse.Result.CREATED == index.getResult());
    }

    /**
     * 根据id获取
     * @param id
     * @return
     * @throws IOException
     */
    @GetMapping("/getIndex/{id}")
    public EsTest getIndex(@PathVariable String id) throws IOException {
        GetRequest request = new GetRequest(esIndex, id);
        GetResponse response = restHighLevelClient.get(request, RequestOptions.DEFAULT);
        System.out.println(response.getSourceAsString());
        EsTest esTest = new ObjectMapper().readValue(response.getSourceAsString(), EsTest.class);
        return esTest;
    }

    /**
     * 根据ID更新
     * @param id
     * @throws IOException
     */
    @GetMapping("/updateIndex/{id}")
    public void updateIndex(@PathVariable String id) throws IOException {
        UpdateRequest request = new UpdateRequest(esIndex, id);
        EsTest esTest = getIndex(id);
        esTest.setName("赵二");
        request.doc(new ObjectMapper().writeValueAsString(esTest), XContentType.JSON);
        request.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        UpdateResponse update = restHighLevelClient.update(request, RequestOptions.DEFAULT);
        System.out.println(update.getGetResult());
        System.out.println(update.status());
    }

    /**
     * 根据id删除
     * @throws IOException
     */
    @GetMapping("/deleteDoc")
    public void deleteDoc() throws IOException {
        DeleteRequest request = new DeleteRequest(esIndex,  "faf086dc-9a26-47ad-a249-a27fe83521ea");
        DeleteResponse delete = restHighLevelClient.delete(request, RequestOptions.DEFAULT);
        System.out.println(delete.status());
    }

    /**
     * 根据id删除
     * @param id
     * @throws IOException
     */
    @GetMapping("/deleteDoc2/{id}")
    public void deleteDoc2(@PathVariable String id) throws IOException {
        BulkRequest request = new BulkRequest();
        request.add(new DeleteRequest(esIndex, id));
        request.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        BulkResponse bulk = restHighLevelClient.bulk(request, RequestOptions.DEFAULT);
        System.out.println(bulk.status());
    }

    /**
     * 简单分页查询
     * @param from
     * @param size
     * @throws IOException
     */
    @GetMapping("/queryPage")
    public void queryPage(Integer from, Integer size) throws IOException {
        SearchRequest request = new SearchRequest(esIndex);
        SearchSourceBuilder builder = new SearchSourceBuilder();
        builder.from(from==null?0:from);
        builder.size(size);
        request.source(builder);
        SearchResponse search = restHighLevelClient.search(request, RequestOptions.DEFAULT);
        SearchHits hits = search.getHits();
        System.out.println(hits);
        System.out.println(hits.getHits());
        System.out.println(hits.getTotalHits().value);
        for (SearchHit hit : hits.getHits()) {
            System.out.println(stringToEntity(hit.getSourceAsString()));
        }
    }

    /**
     * 简单的match查询
     * @param tags
     * @throws IOException
     */
    @GetMapping("/queryMatch")
    public void queryMatch(String tags) throws IOException {
        SearchRequest request = new SearchRequest(esIndex);
        SearchSourceBuilder builder = new SearchSourceBuilder();
        builder.query(QueryBuilders.matchQuery("tags", tags));
        request.source(builder);
        SearchResponse search = restHighLevelClient.search(request, RequestOptions.DEFAULT);
        SearchHits hits = search.getHits();
        for (SearchHit hit : hits) {
            System.out.println(stringToEntity(hit.getSourceAsString()));
        }
    }

    /**
     * bool查询
     * @param tags
     * @param desc
     * @throws IOException
     */
    @GetMapping("boolQuery")
    public void boolQuery(String tags, String desc) throws IOException {
        SearchRequest request = new SearchRequest(esIndex);
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        boolQueryBuilder.should(QueryBuilders.matchQuery("tags",tags))
                .should(QueryBuilders.matchQuery("desc",desc));
        query(request, boolQueryBuilder);
    }

    private void query(SearchRequest request, BoolQueryBuilder boolQueryBuilder) throws IOException {
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.query(boolQueryBuilder);
        request.source(sourceBuilder);
        SearchResponse response = restHighLevelClient.search(request, RequestOptions.DEFAULT);
        SearchHits hits = response.getHits();
        for (SearchHit hit : hits) {
            System.out.println(stringToEntity(hit.getSourceAsString()));
        }
    }

    /**
     *
     * @param tags
     * @param desc
     */
    @GetMapping("/boolQuery2")
    public void boolQuery2(String tags, String desc) throws IOException {
        SearchRequest request = new SearchRequest();
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        boolQueryBuilder.must(QueryBuilders.boolQuery()
                .should(QueryBuilders.matchQuery("tags", tags))
                .should(QueryBuilders.matchQuery("desc", desc))
        );
        query(request, boolQueryBuilder);
    }

    private EsTest stringToEntity(String str) {
        if(StringUtils.isBlank(str)) return null;
        try {
            return new ObjectMapper().readValue(str, EsTest.class);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        return null;
    }
}
