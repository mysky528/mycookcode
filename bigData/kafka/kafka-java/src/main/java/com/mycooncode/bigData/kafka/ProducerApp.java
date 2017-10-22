package com.mycooncode.bigData.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * Hello world!
 *
 */
public class ProducerApp
{


    public static void main( String[] args )
    {
        //消息发送方式：异步发送还是同步发送
        boolean isAsync = args.length == 0 || !args[0].trim().equalsIgnoreCase("sync");
        //System.out.println( "Hello World!" );
        Properties props = new Properties();
        //kafka服务端的主机名和端口号
        props.put("bootstrap.servers","localhost:9092");
        props.put("client.id","DemoProducer"); //客户端的ID
        //消息的key和values都是字节数组，为了将Java对象转换为字节数组，可以配置 key.serializer和value.serializer两个序列化器完成转化
        props.put("key.serializer","org.apache.kafka.common.serialization.IntegerSerializer");
        props.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer");

        //生产者的核心类
        KafkaProducer producer = new KafkaProducer(props);

        String topic = "test";
        int messageNo = 1;//消息的key
        while(true){
            String messageStr = "Message_" + messageNo; //消息的value
            long startTime = System.currentTimeMillis();
            if(isAsync){ //异步发送消息
                //第一个参数是ProducerRecord类型的对象，封装了目标Topic，消息的key、消息的value
                //第二个参数是一个Callback对象，当生产者接收到kafka发来的ACK确认消息的时候，会调用此CallBack对象的onCompletion()方法，实现回调功能
                producer.send(new ProducerRecord(topic,messageNo,messageStr),new ProducerAppCallBack(startTime,messageNo,messageStr));
            }else{
                //同步发送
                try {
                    producer.send(new ProducerRecord(topic, messageNo, messageStr)).get();
                    System.out.println("Sent message: ("+messageNo+" ,"+messageStr + " ");
                }catch (InterruptedException e){
                    e.printStackTrace();
                }catch (ExecutionException e)
                {
                    e.printStackTrace();
                }
                ++messageNo;

            }
        }
    }
}
