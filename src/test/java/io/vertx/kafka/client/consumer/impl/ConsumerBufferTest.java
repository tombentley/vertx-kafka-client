package io.vertx.kafka.client.consumer.impl;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.Assert.*;

import org.apache.kafka.common.TopicPartition;
import org.junit.Test;

import io.vertx.kafka.client.consumer.impl.Buffer.PartitionBuffer;

public class ConsumerBufferTest {

  @Test
  public void partitionBuffer() {
    PartitionBuffer<Integer> pb = new PartitionBuffer<>(Collections.emptyList());
    assertEquals(null, pb.next());
    assertEquals(null, pb.next());
    
    pb.add(Collections.singletonList(1));
    assertEquals(Integer.valueOf(1), pb.next());
    assertEquals(null, pb.next());
    
    pb.add(Collections.singletonList(2));
    pb.add(Collections.singletonList(3));
    assertEquals(Integer.valueOf(2), pb.next());
    assertEquals(Integer.valueOf(3), pb.next());
    assertEquals(null, pb.next());
    
    pb.add(Arrays.asList(4, 5));
    assertEquals(Integer.valueOf(4), pb.next());
    assertEquals(Integer.valueOf(5), pb.next());
    assertEquals(null, pb.next());
    
    pb.add(Arrays.asList(7, 8));
    pb.add(Arrays.asList(9, 10));
    assertEquals(Integer.valueOf(7), pb.next());
    assertEquals(Integer.valueOf(8), pb.next());
    pb.pause();
    assertEquals(null, pb.next());
    pb.resume();
    assertEquals(Integer.valueOf(9), pb.next());
    pb.flush();
    assertEquals(null, pb.next());
  }
  
  @Test
  public void consumerBufferNext() {
    Buffer<Integer, Integer> cb = new Buffer<>();
    Integer foo = 0;
    Integer bar = 1;
    cb.add(foo, Arrays.asList(20, 21, 22, 23, 24, 25));
    cb.add(bar, Arrays.asList(90, 91, 92, 93, 94, 95));
    assertEquals(Integer.valueOf(20), cb.next());
    assertEquals(Integer.valueOf(21), cb.next());
    assertEquals(Integer.valueOf(22), cb.next());
    assertEquals(Integer.valueOf(23), cb.next());
    assertEquals(Integer.valueOf(24), cb.next());
    assertEquals(Integer.valueOf(25), cb.next());
    assertEquals(Integer.valueOf(90), cb.next());
    assertEquals(Integer.valueOf(91), cb.next());
    assertEquals(Integer.valueOf(92), cb.next());
    assertEquals(Integer.valueOf(93), cb.next());
    assertEquals(Integer.valueOf(94), cb.next());
    assertEquals(Integer.valueOf(95), cb.next());
    assertEquals(null, cb.next());
  }
  
  @Test
  public void consumerBufferAdd() {
    Buffer<Integer, Integer> cb = new Buffer<>();
    Integer foo = 0;
    Integer bar = 1;
    cb.add(foo, Arrays.asList(20, 21, 22));
    cb.add(bar, Arrays.asList(90, 91, 92));
    assertEquals(Integer.valueOf(20), cb.next());
    assertEquals(Integer.valueOf(21), cb.next());
    assertEquals(Integer.valueOf(22), cb.next());
    assertEquals(Integer.valueOf(90), cb.next());
    assertEquals(Integer.valueOf(91), cb.next());
    cb.add(bar, Arrays.asList(93, 94, 95));
    assertEquals(Integer.valueOf(92), cb.next());
    cb.add(foo, Arrays.asList(23, 24, 25));
    assertEquals(Integer.valueOf(93), cb.next());
    assertEquals(Integer.valueOf(94), cb.next());
    assertEquals(Integer.valueOf(95), cb.next());
    assertEquals(Integer.valueOf(23), cb.next());
    assertEquals(Integer.valueOf(24), cb.next());
    assertEquals(Integer.valueOf(25), cb.next());
    assertEquals(null, cb.next());
  }
  
  @Test
  public void consumerBufferPause() {
    Buffer<Integer, Integer> cb = new Buffer<>();
    Integer foo = 0;
    Integer bar = 1;
    cb.add(foo, Arrays.asList(20, 21, 22));
    cb.add(bar, Arrays.asList(90, 91, 92));
    assertEquals(Integer.valueOf(20), cb.next());
    cb.pause(foo);
    assertEquals(Integer.valueOf(90), cb.next());
    cb.resume(foo);
    assertEquals(Integer.valueOf(91), cb.next());
    cb.add(bar, Arrays.asList(93, 94, 95));
    assertEquals(Integer.valueOf(92), cb.next());
    cb.add(foo, Arrays.asList(23, 24, 25));
    assertEquals(Integer.valueOf(93), cb.next());
    assertEquals(Integer.valueOf(94), cb.next());
    assertEquals(Integer.valueOf(95), cb.next());
    assertEquals(Integer.valueOf(21), cb.next());
    assertEquals(Integer.valueOf(22), cb.next());
    assertEquals(Integer.valueOf(23), cb.next());
    assertEquals(Integer.valueOf(24), cb.next());
    assertEquals(Integer.valueOf(25), cb.next());
    assertEquals(null, cb.next());
  }
  
  @Test
  public void consumerBufferFlush() {
    Buffer<Integer, Integer> cb = new Buffer<>();
    Integer foo = 0;
    Integer bar = 1;
    cb.add(foo, Arrays.asList(20, 21, 22));
    cb.add(bar, Arrays.asList(90, 91, 92));
    assertEquals(Integer.valueOf(20), cb.next());
    cb.flush(foo);
    assertEquals(Integer.valueOf(90), cb.next());
    assertEquals(Integer.valueOf(91), cb.next());
    cb.add(bar, Arrays.asList(93, 94, 95));
    assertEquals(Integer.valueOf(92), cb.next());
    cb.add(foo, Arrays.asList(23, 24, 25));
    assertEquals(Integer.valueOf(93), cb.next());
    assertEquals(Integer.valueOf(94), cb.next());
    assertEquals(Integer.valueOf(95), cb.next());
    assertEquals(Integer.valueOf(23), cb.next());
    assertEquals(Integer.valueOf(24), cb.next());
    assertEquals(Integer.valueOf(25), cb.next());
    assertEquals(null, cb.next());
  }
}
