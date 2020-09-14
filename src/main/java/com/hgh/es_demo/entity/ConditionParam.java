package com.hgh.es_demo.entity;

import lombok.Data;

@Data
public class ConditionParam {

    private String key;

    private Object value;

    private EsCondition condition;

}
