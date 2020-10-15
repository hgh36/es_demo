package com.hgh.es_demo.component.vo;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.experimental.Accessors;

import java.util.ArrayList;
import java.util.List;

/**
 * 排序条件
 */
@Data
@EqualsAndHashCode(callSuper = false)
@Accessors(chain = true)
public class EsRequestVO {

    /**
     * 查询的索引
     */
    private String index;

    /**
     * 当前页
     */
    private Integer pageIndex = Integer.valueOf(0);

    /**
     * 分页条数
     */
    private Integer pageSize = Integer.valueOf(10);

    /**
     * 参数查询条件+过滤条件
     */
    private List<FilterQueryVO> filterQueryVOList = new ArrayList<>();

    /**
     * 查询返回的字段
     */
    private List<String> returnFields = new ArrayList<>();

    /**
     * 查询过滤掉的字段
     */
    private List<String> excludeFields = new ArrayList<>();

    /**
     * 高亮的字段
     */
    private List<String> highLightKeys = new ArrayList<>();

    /**
     * 排序
     */
    private List<SortOrderVO> sort = new ArrayList<>();
}
