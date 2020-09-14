package com.hgh.es_demo.entity;

import lombok.Data;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Data
public class EsResponse {

    private List<String> jsonObject = new ArrayList<>();

    private List<Map<String, String>> highLightValue = new ArrayList<>();

}
