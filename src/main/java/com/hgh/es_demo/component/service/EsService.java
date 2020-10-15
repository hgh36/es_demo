package com.hgh.es_demo.component.service;

import com.hgh.es_demo.component.vo.EsRequestVO;
import com.hgh.es_demo.component.vo.EsResponseVO;

/**
 * es通用查询
 */
public interface EsService {

    /**
     * 统计
     * @param requestVO
     * @return
     */
    Long count(EsRequestVO requestVO);

    /**
     * 查询
     * @param requestVO
     * @return
     */
    EsResponseVO query(EsRequestVO requestVO);

}
