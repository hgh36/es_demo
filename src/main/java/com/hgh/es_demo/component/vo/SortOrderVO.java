package com.hgh.es_demo.component.vo;

import com.hgh.es_demo.component.enums.SortOrderEnum;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.experimental.Accessors;

/**
 * 排序条件
 */
@Data
@EqualsAndHashCode(callSuper = false)
@Accessors(chain = true)
public class SortOrderVO {

    /**
     * 排序值
     */
    private String key;

    /**
     * 排序类型  acs/desc
     */
    private SortOrderEnum sortOrderEnum;

}
