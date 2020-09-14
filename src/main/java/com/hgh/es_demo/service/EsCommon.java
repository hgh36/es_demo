package com.hgh.es_demo.service;

import com.hgh.es_demo.entity.ConditionParam;
import com.hgh.es_demo.entity.EsRequestParam;
import com.hgh.es_demo.entity.EsResponse;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

import java.io.IOException;
import java.util.List;
import java.util.Map;

@Component
public class EsCommon {

    @Autowired
    private RestHighLevelClient restHighLevelClient;

    public Boolean save(String id, String jsonValue, String esIndex) {
        Assert.notNull(id, "id不能为空！");
        Assert.notNull(jsonValue, "数据不能为空！");
        Assert.notNull(esIndex, "es索引不能为空!");
        IndexRequest request = new IndexRequest(esIndex);
        request.id(id);
        request.source(jsonValue, XContentType.JSON);
        try {
            IndexResponse response = restHighLevelClient.index(request, RequestOptions.DEFAULT);
            return DocWriteResponse.Result.CREATED == response.getResult();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return Boolean.FALSE;
    }

    public Boolean update(String id, String jsonValue, String esIndex) {
        Assert.notNull(id, "id不能为空！");
        Assert.notNull(jsonValue, "数据不能为空！");
        Assert.notNull(esIndex, "es索引不能为空!");
        UpdateRequest request = new UpdateRequest(esIndex, id);
        request.doc(jsonValue, XContentType.JSON);
        request.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        try {
            UpdateResponse response = restHighLevelClient.update(request, RequestOptions.DEFAULT);
            return DocWriteResponse.Result.UPDATED == response.getResult();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return Boolean.FALSE;
    }

    public Boolean remove(String id, String esIndex) {
        Assert.notNull(id, "id不能为空！");
        Assert.notNull(esIndex, "es索引不能为空!");
        DeleteRequest request = new DeleteRequest(esIndex, id);
        try {
            DeleteResponse response = restHighLevelClient.delete(request, RequestOptions.DEFAULT);
            return DocWriteResponse.Result.DELETED == response.getResult();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return Boolean.FALSE;
    }

    public String getJsonObjById(String id, String esIndex) {
        Assert.notNull(id, "id不能为空！");
        Assert.notNull(esIndex, "es索引不能为空!");
        GetRequest request = new GetRequest(esIndex, id);
        try {
            GetResponse response = restHighLevelClient.get(request, RequestOptions.DEFAULT);
            return response.getSourceAsString();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    public EsResponse query(EsRequestParam param) {
        int size = calSize(param.getPageSize());
        int from = calFrom(size, param.getPageIndex());

        SearchSourceBuilder builder = new SearchSourceBuilder();
        builder.from(from).size(size);

        if(param.getSort() != null && !param.getSort().isEmpty()) {
            for(Map.Entry<String, SortOrder> entry: param.getSort().entrySet()) {
                builder.sort(entry.getKey(), entry.getValue());
            }
        }

        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();

        if(param.getMustList() != null && !param.getMustList().isEmpty()) {
            for (ConditionParam conditionParam : param.getMustList()) {
                QueryBuilder queryBuilder = genQueryBuilder(conditionParam);
                if(queryBuilder != null) {
                    boolQueryBuilder.must(queryBuilder);
                }
            }
        }

        if(param.getShouldList() != null && !param.getShouldList().isEmpty()) {
            for (ConditionParam conditionParam : param.getShouldList()) {
                QueryBuilder queryBuilder = genQueryBuilder(conditionParam);
                if(queryBuilder != null) {
                    boolQueryBuilder.should(queryBuilder);
                }
            }
        }

        if(param.getMustNotList() != null && !param.getMustNotList().isEmpty()) {
            for (ConditionParam conditionParam : param.getMustNotList()) {
                QueryBuilder queryBuilder = genQueryBuilder(conditionParam);
                if(queryBuilder != null) {
                    boolQueryBuilder.mustNot(queryBuilder);
                }
            }
        }

        builder.query(boolQueryBuilder);

        HighlightBuilder highlightBuilder = getHighlightBuilder(param.getHighLightKeys());
        if(highlightBuilder != null) {
            builder.highlighter(highlightBuilder);
        }

        SearchRequest request = new SearchRequest(param.getEsIndex());
        request.source(builder);

        EsResponse esResponse = new EsResponse();
        try {
            SearchResponse response = restHighLevelClient.search(request, RequestOptions.DEFAULT);
            SearchHits hits = response.getHits();
            for (SearchHit hit : hits) {
                esResponse.getJsonObject().add(hit.getSourceAsString());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return esResponse;
    }

    HighlightBuilder getHighlightBuilder(List<String> highLightKeys) {
        if(highLightKeys != null || highLightKeys.isEmpty()) return null;
        HighlightBuilder highlightBuilder = new HighlightBuilder().requireFieldMatch(false);
        for (String highLightKey : highLightKeys) {
            highlightBuilder.field(highLightKey);
        }
        highlightBuilder.preTags("<span style=\"color:red\">");
        highlightBuilder.postTags("</span>");
        return highlightBuilder;
    }

    private QueryBuilder genQueryBuilder(ConditionParam conditionParam) {
        QueryBuilder queryBuilder = null;
        switch (conditionParam.getCondition()) {
            case TERM:
                queryBuilder = QueryBuilders.termQuery(conditionParam.getKey(), conditionParam.getValue());
                break;
            case TERMS:
                queryBuilder = QueryBuilders.termsQuery(conditionParam.getKey(), conditionParam.getValue());
                break;
            case EXISTS_QUERY:
                queryBuilder = QueryBuilders.existsQuery(conditionParam.getKey());
                break;
            case MATCH_PHRASE:
                queryBuilder = QueryBuilders.matchPhraseQuery(conditionParam.getKey(), conditionParam.getValue());
                break;
            default:
                break;

        }
        return queryBuilder;
    }


    public int calFrom(Integer pageSize, Integer pageIndex) {
        Assert.notNull(pageSize, "分页条数不能为空！");
        Assert.notNull(pageIndex, "当前页码不能为空！");
        return pageIndex>0?(pageIndex-1)*pageSize:0;
    }

    public int calSize(Integer pageSize) {
        Assert.notNull(pageSize, "分页条数不能为空！");
        return pageSize>10000?10000:pageSize;
    }
}
