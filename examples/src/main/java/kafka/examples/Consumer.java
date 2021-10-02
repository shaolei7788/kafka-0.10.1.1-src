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

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Collections;
import java.util.Properties;

public class Consumer  {
    private final KafkaConsumer<Integer, String> consumer;

    private final String topic;

    public static void main(String[] args) {
        Consumer consumer = new Consumer("four");
        consumer.doWork();
    }

    public Consumer(String topic) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop5:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "Demo1");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        //最大拉取时间间隔300s = 5 min
        //props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, "60000");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        consumer = new KafkaConsumer<>(props);
        this.topic = topic;
    }

    public void doWork() {
        //todo subscribe 订阅模式
        //todo assign 分配模式
        consumer.subscribe(Collections.singletonList(this.topic));
        while(true){
            //todo Consumer每次发起fetch请求时，读取到的数据是有限制的 通过配置项max.partition.fetch.bytes来限制 ，
            // max.partition.fetch.bytes  1 * 1024 * 1024 = 1m  每个分区拉取的最大字节数
            // 而在执行poll方法时，会根据配置项 max.poll.records 默认500 来限制一次最多pool多少个record
            // max.poll.interval.ms = 300000 = 5min
            // session.timeout.ms = 10000 = 10s
            // heartbeat.interval.ms = 3000 = 3s
            // fetch.max.bytes = 50 * 1024 * 1024 = 50m  从一个broker中拉取的最大字节数
            ConsumerRecords<Integer, String> records = consumer.poll( 30 * 1000);
            for (ConsumerRecord<Integer, String> record : records) {
                System.out.println("Received message: (" + record.partition() + record.key() + ", " + record.value() + ") at offset " + record.offset());
            }
        }

    }

}
