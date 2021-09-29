/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package kafka.examples;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class Producer {
    private final KafkaProducer<Integer, String> producer;
    private final String topic;
    private final Boolean isAsync;


    public static void main(String[] args) {
        Producer producerThread = new Producer(KafkaProperties.TOPIC, true);
        try {
            Thread.sleep(100000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        //producerThread.produce();
    }

    /**
     * 构建方法,初始化生产者对象
     * @param topic 主题名称
     * @param isAsync 异步/同步 标记
     */
    public Producer(String topic, Boolean isAsync) {
        //kafka生产者客户端KafkaProducer有3个参数是必须配置的: bootstrap.servers key.serializer value.serializer
        //配置必要的参数
        Properties props = new Properties();
        //指定生产者客户端连接kafka集群的broker地址清单,可以设置一个或多个(集群会同步消息)
        //建议至少设置2个地址,当其中任意一个宕机的时候,生产者仍然可以连接到kafka的集群
        props.put("bootstrap.servers", "hadoop1:9092");
        //设定kafkaProducer对应的客户端id,默认值是空,如果客户端不设置,那么kafkaProducer会自动生成一个非空字符串,内容形式是 "process-1"
        props.put("client.id", "DemoProducer");
        //设置序列化的类,broker端接收的消息必须以字节数组的(byte[])的形式存在
        //指定的序列化器必须是全限定名
        props.put("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        //消费者 ,消费数据的时候,就要进行反序列化
        //TODO 初始化KafkaProducer过程中都干了些什么: 初始化了一堆参数;初始化了几个核心的对象
        producer = new KafkaProducer<>(props);
        this.topic = topic;
        this.isAsync = isAsync;
    }

    //发送消息的三种方式 : 发后即忘,同步,异步
    //send()方法有两种重载方式,其本身就是异步的,如果想要实现同步,可以利用send()方法返回的Future对象实现
    public void produce() {
        //定义一个变量
        int messageNo = 1;
        // 一直往kafka发送数据
        //while (true) {
            //模拟生成一条消息
            String messageStr = "Message_" + messageNo;
            //获取当前时间戳
            long startTime = System.currentTimeMillis();
            // isAsync,kafka消费数据的时候,有两种方式
            // 1. 异步发送
            // 2. 同步发送
            // isAsync true的时候是异步发送 false的时候是同步发送
            if (isAsync) { // Send asynchronously
                //todo 异步发送 , 一直发送,消息响应结果交给回调函数处理
                //这样做的好处,性能比较好,我们的生产环境就用这种方式
                //在send()方法里构建消息对象ProducerRecord,topic和value是必填项
                //回调函数的作用,判断有没有异常,如果有异常,分为2种情况: 1.不可重试异常,直接返回异常到客户端,再进行处理;2.可以重试异常,重新加入缓存里面,再发送出去
                producer.send(new ProducerRecord<>(topic, messageNo, messageStr), new DemoCallBack(startTime, messageNo, messageStr));

                //producer.send(record1,callback1);
                //producer.send(record2,callback2);
                //对于同一个分区而言,如果record1先于record2发送,那么kakfaProducer会保证对应的callback1先于callback2调用
                //也就是说,回调函数也可以保证分区内有序
                //一般在发送完消息之后,需要调用producer的close()方法来回收资源

            } else { // Send synchronously
                try {
                    //同步方式
                    //发送一条消息,等待这条消息的所有后续工作完成之后,再继续发送下一条消息
                    //Future对象对象可以使调用方稍后获得发送的结果
                    //send(xxx).get(),直链式的方式可以阻塞等待kafka的响应,直到消息发送成功或被捕获异常
                    producer.send(new ProducerRecord<>(topic, messageNo, messageStr)).get();
                    System.out.println("Sent message: (" + messageNo + ", " + messageStr + ")");
                } catch (InterruptedException | ExecutionException e) {
                    e.printStackTrace();
                }
            }
            ++messageNo;
       // }
    }
}

class DemoCallBack implements Callback {

    private final long startTime;
    private final int key;
    private final String message;

    public DemoCallBack(long startTime, int key, String message) {
        this.startTime = startTime;
        this.key = key;
        this.message = message;
    }

    /**
     * A callback method the user can implement to provide asynchronous handling of request completion. This method will
     * be called when the record sent to the server has been acknowledged. Exactly one of the arguments will be
     * non-null.
     *
     * @param metadata  The metadata for the record that was sent (i.e. the partition and offset). Null if an error
     *                  occurred.
     * @param exception The exception thrown during processing of this record. Null if no error occurred.
     */
    //onCompletion()方法中两个参数是互斥的,当一个不是null的时候,另一个必定是null
    public void onCompletion(RecordMetadata metadata, Exception exception) {
        long elapsedTime = System.currentTimeMillis() - startTime;
        //没有异常
        if (metadata != null) {
            System.out.println(
                    //一般我们的生产里面,还会有其他的备用的链路
                "message(" + key + ", " + message + ") sent to partition(" + metadata.partition() +
                    "), " +
                    "offset(" + metadata.offset() + ") in " + elapsedTime + " ms");
        } else {
            exception.printStackTrace();
        }
    }
}
