package com.zhibo8.warehouse.commons;

import org.apache.commons.beanutils.BeanUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.util.Map;

public class BeanUtil {
    private static Logger logger = LoggerFactory.getLogger(BeanUtil.class);

    public static Map<String, Object> transBean2Map(Object obj) {

        if (obj == null) {
            return null;
        }
        Map map = null;
        try {
            map = BeanUtils.describe(obj);
            map.remove("class");
        } catch (IllegalAccessException e) {
            logger.error(e.getMessage(), e);
        } catch (InvocationTargetException e) {
            logger.error(e.getMessage(), e);
        } catch (NoSuchMethodException e) {
            logger.error(e.getMessage(), e);
        }
        return map;
    }
}
