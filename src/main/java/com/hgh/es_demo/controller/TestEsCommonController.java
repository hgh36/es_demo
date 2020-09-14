package com.hgh.es_demo.controller;

import com.hgh.es_demo.entity.ConditionParam;
import com.hgh.es_demo.entity.EsCondition;
import com.hgh.es_demo.entity.EsRequestParam;
import com.hgh.es_demo.entity.EsResponse;
import com.hgh.es_demo.service.EsCommon;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class TestEsCommonController {

    @Autowired
    private EsCommon esCommon;

    @GetMapping("/common/test")
    public EsResponse test(){
        EsRequestParam requestParam = new EsRequestParam();
        requestParam.setEsIndex("ssmo_resource_test");

        ConditionParam param = new ConditionParam();
        param.setCondition(EsCondition.TERM);
        param.setKey("parentId");
        param.setValue(270127975461163009L);
        requestParam.getMustList().add(param);

        return esCommon.query(requestParam);
    }


}
