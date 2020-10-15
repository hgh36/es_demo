package com.hgh.es_demo.component.vo;

import io.swagger.annotations.ApiModelProperty;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.experimental.Accessors;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * ES查询返回类
 */
@Data
@EqualsAndHashCode(callSuper = false)
@Accessors(chain = true)
public class EsResponseVO {

    /**
     * 返回的json字符串，需自己转换
     */
    @ApiModelProperty(value = "返回的json字符串，需自己转换")
    private List<String> jsonObject = new ArrayList<>();

    /**
     * 查询的高亮字段，默认<span></span>
     */
    @ApiModelProperty(value = "查询的高亮字段，默认<span></span>")
    private List<Map<String, String>> highLightValue = new ArrayList<>();

    /**
     * 统计总条数
     */
    @ApiModelProperty(value = "统计总条数")
    private Long totalCount;

}
