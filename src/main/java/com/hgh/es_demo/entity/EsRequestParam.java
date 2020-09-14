package com.hgh.es_demo.entity;

import lombok.Data;
import org.elasticsearch.search.sort.SortOrder;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Data
public class EsRequestParam {

    private String esIndex;

    private List<ConditionParam> mustList = new ArrayList<>();

    private List<ConditionParam> shouldList = new ArrayList<>();

    private List<ConditionParam> mustNotList = new ArrayList<>();

    private List<String> highLightKeys = new ArrayList<>();

    private Map<String, SortOrder> sort = new HashMap<>();

    private Integer pageIndex = Integer.valueOf(0);

    private Integer pageSize = Integer.valueOf(10);

    private List<String> returnFields = new ArrayList<>();

    private List<String> excludeFields = new ArrayList<>();

}
