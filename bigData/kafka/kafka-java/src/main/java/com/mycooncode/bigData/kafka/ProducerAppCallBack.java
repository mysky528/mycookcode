package com.mycooncode.bigData.kafka;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringDeserializer;

/**
 * Created by zhaolu on 2017/10/22.
 */
public class ProducerAppCallBack implements Callback {

    private final long startTime;//开始发送的时间戳

    private final int key; //消息的key

    private  final String message;//消息的value

    public ProducerAppCallBack(long startTime,int key,String message)
    {
        this.startTime = startTime;
        this.key = key;
        this.message = message;
    }

    /**
     * 生产者成功发送消息，收到Kafka服务端发来的ACK确认消息后，会调用此回调函数
     * @param metadata 生产者发送的消息的元数据，如果发送过程中出现异常，此参数为null
     * @param exception 发送过程中出现的异常，如果发送成功，则此参数为null
     */
    public void onCompletion(RecordMetadata metadata,Exception exception)
    {
        long elapsedTime = System.currentTimeMillis() - startTime;
        if(metadata != null)
        {
            //RecordMetadata中包含了分区信息、Offset信息等
            System.out.println("message("+key+", "+ message + ") sent to partition(" + metadata.partition()+"), "+"offset("+
            metadata.offset()+") in "+elapsedTime+" ms");

        }else
        {
            exception.printStackTrace();
        }
    }

}
