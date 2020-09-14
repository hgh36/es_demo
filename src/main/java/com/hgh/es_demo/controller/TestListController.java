package com.hgh.es_demo.controller;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hgh.es_demo.entity.EsTest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.UpdateByQueryRequest;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;
import java.util.*;

@RestController
public class TestListController {

    @Value("${es.index.test_list}")
    private String esIndex;
    @Value("${es.index.my_test}")
    private String myTest;

    @Autowired
    private RestHighLevelClient restHighLevelClient;

    @GetMapping("/testList/add")
    public void add() throws IOException {
        EsTest esTest = new EsTest();
        esTest.setId(UUID.randomUUID().toString());
        esTest.setName("张三");
        esTest.setDesc("this is test doc");
        esTest.setTags(Arrays.asList("java","大数据","elastic"));
        Set<Long> ids = new HashSet<>();
        ids.add(1288755497016520705L);
        ids.add(120000010000001001L);
        ids.add(120000010000001002L);
        esTest.setIds(ids);

        IndexRequest request = new IndexRequest(esIndex);
        request.id(esTest.getId());
        request.source(new ObjectMapper().writeValueAsString(esTest), XContentType.JSON);
        IndexResponse index = restHighLevelClient.index(request, RequestOptions.DEFAULT);
    }

    @GetMapping("/testList/updateByQuery")
    public void updateByQuery() throws IOException {
        List<String> idLst = new ArrayList<>();
        idLst.add("d4eff71d-cf74-4421-a2b1-b506b36bbb97");
        idLst.add("a9441d1d-8cb6-4532-a93c-ac2c2c1cc0c6");
        UpdateByQueryRequest request = new UpdateByQueryRequest("test_list");
//        BoolQueryBuilder queryBuilder = new BoolQueryBuilder();
//        queryBuilder.must(QueryBuilders.termsQuery("id", idLst));
        request.setQuery(QueryBuilders.termsQuery("_id", idLst));
        request.setBatchSize(10);
        request.setTimeout(new TimeValue(60000));

        Set<Long> ids = new HashSet<>();
//        ids.add(120000010000000001L);
//        ids.add(120000010000000002L);
//
//        ids.add(120000010000000003L);
//        ids.add(120000010000000004L);

        HashMap<String,Object> data=new HashMap<>();
        data.put("desc", "this is test desc is long long long.....");
        data.put("ids", ids);

//        List<Map<String, Object>> listMap = new ArrayList<>();
//        for (Long id : ids) {
//            Map<String, Object> map = new HashMap<>();
//            map.put("ids", id);
//            listMap.add(map);
//        }

        String a = "李四是李四123";
            //追加
//        request.setScript(new Script(ScriptType.INLINE, "painless",
//                "ctx._source.name='"+a+"'"+
//                        ";ctx._source.desc='desc is test'"+
////                        ";ctx._source.ids.='"+listMap+"'"
//                        ";ctx._source.ids.add("+123456+")"
//                , Collections.emptyMap()));
            //替换
//        request.setScript(new Script(ScriptType.INLINE, "painless",
//                        "ctx._source.ids=params.ids;ctx._source.desc=params.desc",
//                        data));

        //先删再加
        data.put("ids", 888888);
        request.setScript(new Script(ScriptType.INLINE, "painless",
                "ctx._source.name='"+a+"'"+
                        ";ctx._source.desc='desc is test remove and add'"+
                        ";if(!ctx._source.ids.contains(params.ids)){ctx._source.ids.add(555555)}"+
                        ";if(!ctx._source.ids.contains(params.ids)){ctx._source.ids.add(999999)}"
                , data));

        BulkByScrollResponse bulkByScrollResponse = restHighLevelClient.updateByQuery(request, RequestOptions.DEFAULT);
        System.out.println(bulkByScrollResponse);
        System.out.println(bulkByScrollResponse.getUpdated());
    }

    @GetMapping("/testList/updateByQueryRemove")
    public void updateByQueryRemove() throws IOException {
        List<String> idLst = new ArrayList<>();
        idLst.add("d4eff71d-cf74-4421-a2b1-b506b36bbb97");
        idLst.add("a9441d1d-8cb6-4532-a93c-ac2c2c1cc0c6");
        UpdateByQueryRequest request = new UpdateByQueryRequest("test_list");
        request.setQuery(QueryBuilders.termsQuery("_id", idLst));
        request.setBatchSize(10);
        request.setTimeout(new TimeValue(60000));

        HashMap<String,Object> data=new HashMap<>();
        data.put("desc", "this is test desc is remove.....");

//        //先删再加
//        data.put("ids", 111111);
//        request.setScript(new Script(ScriptType.INLINE, "painless",
//                "ctx._source.name='test'"+
//                        ";ctx._source.desc='desc is test remove and add'"+
//                        ";if(ctx._source.ids.contains(params.ids)){ctx._source.ids.remove(ctx._source.ids.indexOf(params.ids))}"
//                , data));
        List<Long> ids = new ArrayList<>();
        ids.add(6L);
        ids.add(7L);
        ids.add(8L);
        ids.add(9L);
        //先删再加
        data.put("ids", ids);
        request.setScript(new Script(ScriptType.INLINE, "painless",
                "ctx._source.name='test'"+
                        ";ctx._source.desc='desc is test remove and add'"+
                        ";ctx._source.ids=params.ids"
                , data));

        BulkByScrollResponse bulkByScrollResponse = restHighLevelClient.updateByQuery(request, RequestOptions.DEFAULT);
        System.out.println(bulkByScrollResponse);
        System.out.println(bulkByScrollResponse.getUpdated());
    }

    @GetMapping("/testList/query")
    public void query() throws IOException {
        List<String> idLst = new ArrayList<>();
        idLst.add("d4eff71d-cf74-4421-a2b1-b506b36bbb97");
        idLst.add("a9441d1d-8cb6-4532-a93c-ac2c2c1cc0c6");
        SearchRequest request = new SearchRequest(esIndex);
        SearchSourceBuilder builder = new SearchSourceBuilder();
        builder.size(1000);
        builder.from(1);
        builder.query(QueryBuilders.termsQuery("_id", idLst));
        request.source(builder);
        SearchResponse search = restHighLevelClient.search(request, RequestOptions.DEFAULT);
        SearchHits hits = search.getHits();
        System.out.println(hits.getHits());
    }
}
