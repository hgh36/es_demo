package com.hgh.es_demo.component.vo;

import com.hgh.es_demo.component.enums.ParamQueryConditionEnum;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.experimental.Accessors;

/**
 * 参数查询条件
 */
@Data
@EqualsAndHashCode(callSuper = false)
@Accessors(chain = true)
public class ParamQueryVO {

    /**
     * 字段名称
     */
    private String key;

    /**
     * 条件
     */
    private ParamQueryConditionEnum conditionEnum;

    /**
     * 查询值
     */
    private String value;

    /**
     * 查询值-扩展，用于between
     */
    private String valueExt;

}
