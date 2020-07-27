package com.hgh.es_demo.entity;

import lombok.Data;

import java.util.List;

@Data
public class EsTest {

    private String id;

    private String name;

    private String desc;

    private List<String> tags;

}
