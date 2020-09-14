package com.hgh.es_demo.controller;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.range.Range;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.metrics.Avg;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

@RestController
public class ResourceTestController {

    @Value("${es.index.my_resource}")
    private String esIndex;

    @Value("${es.index.hgh}")
    private String hgh;

    @Autowired
    private RestHighLevelClient restHighLevelClient;

    /*简单分组*/
    @GetMapping("/resource/aggResType")
    public void aggResType(){
        SearchRequest request = new SearchRequest(esIndex);

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        AggregationBuilder aggregationBuilder = AggregationBuilders
                .terms("agg_resType")
                .field("resType");

        searchSourceBuilder.aggregation(aggregationBuilder);
        request.source(searchSourceBuilder);

        try {
            SearchResponse response = restHighLevelClient.search(request, RequestOptions.DEFAULT);
            Terms agg = response.getAggregations().get("agg_resType");
            System.out.println(agg);
            System.out.println(agg.getBuckets().size());
            for (Terms.Bucket bucket : agg.getBuckets()) {
                System.out.println(bucket.getKey());
                System.out.println(bucket.getDocCount());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /*范围分组*/
    @GetMapping("/resource/aggRange")
    public void aggRange(){
        SearchRequest request = new SearchRequest(hgh);

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        AggregationBuilder aggregationBuilder = AggregationBuilders
                .range("agg_age")
                .field("age")
                .addRange(18, 26);

        searchSourceBuilder.aggregation(aggregationBuilder);
        request.source(searchSourceBuilder);

        try {
            SearchResponse response = restHighLevelClient.search(request, RequestOptions.DEFAULT);
            Range agg = response.getAggregations().get("agg_age");
            System.out.println(agg);
            System.out.println(agg.getBuckets().size());
            for (Range.Bucket bucket : agg.getBuckets()) {
                System.out.println(bucket.getKey());
                System.out.println(bucket.getDocCount());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /*查询条件过滤后再分组*/
    @GetMapping("/resource/aggAge")
    public void aggAge(){
        SearchRequest request = new SearchRequest(hgh);

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        AggregationBuilder aggregationBuilder = AggregationBuilders.avg("agg_age").field("age");

        searchSourceBuilder.query(QueryBuilders.rangeQuery("age").gte(18));
        searchSourceBuilder.aggregation(aggregationBuilder);
        request.source(searchSourceBuilder);

        try {
            SearchResponse response = restHighLevelClient.search(request, RequestOptions.DEFAULT);
            Avg agg = response.getAggregations().get("agg_age");
            System.out.println(agg);
            System.out.println(agg.getName());
            System.out.println(agg.getValue());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /*根据时间进行分组*/
    @GetMapping("/resource/aggTermsDate")
    public void aggTermsDate(){
        SearchRequest request = new SearchRequest(esIndex);

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        AggregationBuilder aggregationBuilder = AggregationBuilders
                .terms("agg_date").field("updateDt");

        searchSourceBuilder.aggregation(aggregationBuilder);
        request.source(searchSourceBuilder);

        try {
            SearchResponse response = restHighLevelClient.search(request, RequestOptions.DEFAULT);
            Terms agg = response.getAggregations().get("agg_date");
            System.out.println(agg);
            System.out.println(agg.getBuckets().size());
            System.out.println("---------");
            DateFormat format = new SimpleDateFormat("yyyy-MM-dd");
            for (Terms.Bucket bucket : agg.getBuckets()) {
                System.out.println(bucket.getKey());
                System.out.println(format.format(new Date(Long.valueOf(bucket.getKeyAsString()))));
                System.out.println(bucket.getDocCount());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /*根据时间范围group by*/
    @GetMapping("/resource/aggRangeDate")
    public void aggRangeDate(){
        SearchRequest request = new SearchRequest(esIndex);

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        AggregationBuilder aggregationBuilder = AggregationBuilders
                .range("agg_date")
//                .format("yyyy-MM-dd")
                .field("updateDt").addRange(1598251850315L, 1598444015894L);

        searchSourceBuilder.aggregation(aggregationBuilder);
        request.source(searchSourceBuilder);

        try {
            SearchResponse response = restHighLevelClient.search(request, RequestOptions.DEFAULT);
            Range agg = response.getAggregations().get("agg_date");
            System.out.println(agg);
            System.out.println(agg.getBuckets().size());

            for (Range.Bucket bucket : agg.getBuckets()) {
                System.out.println(bucket.getKey());
                System.out.println(bucket.getKeyAsString());
                System.out.println(bucket.getDocCount());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        Date date = new Date(1597766400000L);
        DateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:ss:dd");
        System.out.println(format.format(date));
        System.out.println(new Date().getTime());
    }

}
