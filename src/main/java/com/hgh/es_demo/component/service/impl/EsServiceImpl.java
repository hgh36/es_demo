package com.hgh.es_demo.component.service.impl;

import com.hgh.es_demo.component.enums.FilterQueryConditionEnum;
import com.hgh.es_demo.component.service.EsService;
import com.hgh.es_demo.component.vo.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.core.CountRequest;
import org.elasticsearch.client.core.CountResponse;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.elasticsearch.search.sort.SortOrder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.Assert;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

@Slf4j
@Service
public class EsServiceImpl implements EsService {

    @Autowired
    private RestHighLevelClient restHighLevelClient;

    @Override
    public Long count(EsRequestVO requestVO) {
        Assert.notNull(requestVO.getIndex(), "索引不能为空!");
        CountRequest request = new CountRequest(requestVO.getIndex());
        BoolQueryBuilder boolQueryBuilder = genCondition(requestVO);
        request.query(boolQueryBuilder);
        try {
            CountResponse response = restHighLevelClient.count(request, RequestOptions.DEFAULT);
            return response.getCount();
        } catch (IOException e) {
            log.error("统计数据异常:{}", e);
        }
        return 0L;
    }

    @Override
    public EsResponseVO query(EsRequestVO requestVO) {
        Assert.notNull(requestVO.getIndex(), "索引不能为空!");

        int size = calSize(requestVO.getPageSize());
        int from = calFrom(size, requestVO.getPageIndex());

        SearchSourceBuilder builder = new SearchSourceBuilder();
        //设置分页值
        builder.from(from).size(size);

        //设置排序
        genSort(builder, requestVO.getSort());

        //设置高亮
        genHighlightBuilder(builder, requestVO.getHighLightKeys());

        //设置返回和过滤字段
        if((requestVO.getReturnFields() != null && !requestVO.getReturnFields().isEmpty())
                || (requestVO.getExcludeFields() != null && !requestVO.getExcludeFields().isEmpty())) {
            String[] returnFields = new String[]{};
            if(requestVO.getReturnFields() != null && !requestVO.getReturnFields().isEmpty()) {
                returnFields = requestVO.getReturnFields().toArray(new String[requestVO.getReturnFields().size()]);
            }
            String[] exclude = new String[]{};
            if(requestVO.getExcludeFields() != null && !requestVO.getExcludeFields().isEmpty()) {
                exclude = requestVO.getExcludeFields().toArray(new String[requestVO.getExcludeFields().size()]);
            }
            builder.fetchSource(returnFields, exclude);
        }

        //查询
        BoolQueryBuilder boolQueryBuilder = genCondition(requestVO);

        builder.query(boolQueryBuilder);

        SearchRequest request = new SearchRequest(requestVO.getIndex());
        request.source(builder);

        EsResponseVO esResponse = new EsResponseVO();
        try {
            SearchResponse response = restHighLevelClient.search(request, RequestOptions.DEFAULT);
            SearchHits hits = response.getHits();
            Set<String> set = new HashSet<>();
            for (SearchHit hit : hits) {
                set.add(hit.getSourceAsString());

                Map<String, HighlightField> highlightFields = hit.getHighlightFields();
                if(highlightFields != null && highlightFields.size() > 0) {
                    Map<String, String> highLightMap = new HashMap<>();
                    requestVO.getHighLightKeys().forEach(key -> {
                        HighlightField titleField = highlightFields.get(key);
                        if (titleField != null) {
                            Text[] fragments = titleField.fragments();
                            String name2 = "";
                            for (Text text : fragments) {
                                name2 += text;
                            }
                            highLightMap.put(key, name2);
                        }
                    });
                    esResponse.getHighLightValue().add(highLightMap);
                }
            }
            esResponse.setJsonObject(new ArrayList<>(set));
            esResponse.setTotalCount(hits.getTotalHits().value);

        } catch (IOException e) {
            log.error("查询异常：{}", e);
        }
        return esResponse;
    }

    /*排序*/
    private void genSort(SearchSourceBuilder builder, List<SortOrderVO> sort) {
        if(sort != null && !sort.isEmpty()) {
            sort.forEach(order ->
                builder.sort(order.getKey()+".keyword", SortOrder.fromString(order.getSortOrderEnum().name()))
            );
        }
    }

    /*构建查询条件*/
    private BoolQueryBuilder genCondition(EsRequestVO requestVO) {
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        Map<FilterQueryConditionEnum, List<FilterQueryVO>> map = requestVO.getFilterQueryVOList().stream().collect(Collectors.groupingBy(FilterQueryVO::getFilter));

        if(map.containsKey(FilterQueryConditionEnum.AND)) {
            map.get(FilterQueryConditionEnum.AND).forEach(filterQueryVO -> {
                if(filterQueryVO.getParamQueryVO() != null && filterQueryVO.getParamQueryVO().getValue() != null) {
                    boolQueryBuilder.must(genQueryBuilder(filterQueryVO.getParamQueryVO()));
                }
            });
        }

        if(map.containsKey(FilterQueryConditionEnum.OR)) {
            BoolQueryBuilder should = new BoolQueryBuilder();
            map.get(FilterQueryConditionEnum.OR).forEach(filterQueryVO -> {
                if(filterQueryVO.getParamQueryVO() != null && filterQueryVO.getParamQueryVO().getValue() != null) {
                    should.should(genQueryBuilder(filterQueryVO.getParamQueryVO()));
                }
            });
            boolQueryBuilder.must(should);
        }

        return boolQueryBuilder;
    }

    /*构建查询条件*/
    private QueryBuilder genQueryBuilder(ParamQueryVO conditionParam) {
        QueryBuilder queryBuilder = null;
        switch (conditionParam.getConditionEnum()) {
            case EQ:
                queryBuilder = QueryBuilders.termQuery(conditionParam.getKey(), conditionParam.getValue());
                break;
            case NEQ:
                queryBuilder = QueryBuilders.boolQuery()
                        .mustNot(QueryBuilders.termsQuery(conditionParam.getKey(), conditionParam.getValue()));
                break;
            case LIKE:
                queryBuilder = QueryBuilders.matchPhraseQuery(conditionParam.getKey(), conditionParam.getValue());
                break;
            case LT:
                queryBuilder = QueryBuilders.rangeQuery(conditionParam.getKey()).lt(conditionParam.getValue());
                break;
            case LTE:
                queryBuilder = QueryBuilders.rangeQuery(conditionParam.getKey()).lte(conditionParam.getValue());
                break;
            case GT:
                queryBuilder = QueryBuilders.rangeQuery(conditionParam.getKey()).gt(conditionParam.getValue());
                break;
            case GTE:
                queryBuilder = QueryBuilders.rangeQuery(conditionParam.getKey()).gte(conditionParam.getValue());
                break;
            case BETWEEN:
                if(conditionParam.getValueExt() != null && StringUtils.isNotBlank(conditionParam.getValueExt())) {
                    queryBuilder = QueryBuilders.rangeQuery(conditionParam.getKey())
                            .from(conditionParam.getValue())
                            .to(conditionParam.getValueExt());
                } else {
                    queryBuilder = QueryBuilders.rangeQuery(conditionParam.getKey()).gte(conditionParam.getValue());
                }
                break;
            case NOT_NULL:
                queryBuilder = QueryBuilders.boolQuery().must(QueryBuilders.existsQuery(conditionParam.getKey()));
                break;
            case IS_NULL:
                queryBuilder = QueryBuilders.boolQuery().mustNot(QueryBuilders.existsQuery(conditionParam.getKey()));
                break;
            default:
                break;
        }
        return queryBuilder;
    }

    /*高亮字段*/
    private void genHighlightBuilder(SearchSourceBuilder builder, List<String> highLightKeys) {
        if(highLightKeys == null || highLightKeys.isEmpty()) return;

        HighlightBuilder highlightBuilder = new HighlightBuilder();
        HighlightBuilder.Field highlightFields;
        for (String highLightKey : highLightKeys) {
            highlightFields = new HighlightBuilder.Field(highLightKey);
            highlightFields.preTags("<span style=\"color:red\">").postTags("</span>");
            highlightBuilder.fields().add(highlightFields);
        }
        builder.highlighter(highlightBuilder);
    }

    /*分页-起始*/
    public int calFrom(Integer pageSize, Integer pageIndex) {
        Assert.notNull(pageSize, "分页条数不能为空！");
        Assert.notNull(pageIndex, "当前页码不能为空！");
        return pageIndex>0?(pageIndex-1)*pageSize:0;
    }

    /*分页-条数*/
    public int calSize(Integer pageSize) {
        Assert.notNull(pageSize, "分页条数不能为空！");
        return pageSize>10000?10000:pageSize;
    }


}
