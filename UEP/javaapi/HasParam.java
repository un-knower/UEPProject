package com.haiyisoft.bds.javaapi;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by XingxueDU on 2018/3/15.
 */
public abstract class HasParam {
    private HashMap<String,String> defaultMap = new HashMap<>();
    private HashMap<String,String> paramMap = new HashMap<>();

    public HasParam(Map<String,String> defaultParams){
        this.defaultMap.putAll(defaultParams);
    }
    public HasParam(){

    }

    public String getDefaultParam(String key){
        return defaultMap.get(key);
    }
    public String getParam(String key){
        if(paramMap.containsKey(key))return paramMap.get(key);
        else return defaultMap.get(key);
    }

    public void setParam(String key,String value){
        this.paramMap.put(key,value);
    }

    public String[] paramList(){
        return (String[]) defaultMap.keySet().toArray();
    }

    public boolean containsParam(String key){
        return defaultMap.containsKey(key)||paramMap.containsKey(key);
    }
}
