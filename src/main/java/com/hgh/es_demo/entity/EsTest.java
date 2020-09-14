package com.hgh.es_demo.entity;

import lombok.Data;

import java.util.List;
import java.util.Set;

@Data
public class EsTest {

    private String id;

    private String name;

    private String desc;

    private List<String> tags;

    private Set<Long> ids;

}
