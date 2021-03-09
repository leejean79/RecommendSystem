/*
 * Copyright (c) 2017. WuYufei All rights reserved.
 */

package com.leejean;

import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;

/**
 * Created by wuyufei on 2017/6/18.
 */
public class LogProcessor implements Processor<byte[],byte[]> {


    //定义了上下文
    private ProcessorContext context;

    @Override
    public void init(ProcessorContext context) {
        this.context = context;
    }

    //处理数据
    @Override
    public void process(byte[] key, byte[] value) {
        String input = new String(value);
        if(input.contains("MOVIE_RATING_PREFIX:")){
            input = input.split("MOVIE_RATING_PREFIX:")[1].trim();
            context.forward("logProcessor".getBytes(), input.getBytes());
        }
    }

    @Override
    public void punctuate(long timestamp) {
    }

    @Override
    public void close() {
    }
}
