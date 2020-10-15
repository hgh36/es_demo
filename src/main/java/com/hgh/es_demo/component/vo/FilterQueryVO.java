package com.hgh.es_demo.component.vo;

import com.hgh.es_demo.component.enums.FilterQueryConditionEnum;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.experimental.Accessors;

/**
 * 参数查询条件+过滤条件
 */
@Data
@EqualsAndHashCode(callSuper = false)
@Accessors(chain = true)
public class FilterQueryVO {

    /**
     * 参数查询条件
     */
    private ParamQueryVO paramQueryVO;

    /**
     * 过滤条件，and / or
     */
    private FilterQueryConditionEnum filter;

}
