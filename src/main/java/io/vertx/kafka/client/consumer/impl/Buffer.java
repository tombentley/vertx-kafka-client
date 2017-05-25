package io.vertx.kafka.client.consumer.impl;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.kafka.common.TopicPartition;

/**
 * A thread-safe buffer of records each belonging to one of a set of partitions.
 * The records can be collectively {@linkplain #next() iterated} but also 
 * {@linkplain #pause(Object) paused}, 
 * {@linkplain #resume(Object) resumed} and 
 * {@linkplain #flush(Object) flushed} on a per-partition basis.
 *
 * @param <P> The type of the partition
 * @param <R> The type of the record
 */
public class Buffer<P, R> {

  /** 
   * Holds the fetched records for given partition.
   */
  static class PartitionBuffer<T> {

    // We use a List<List> because {@link ConsumerRecords#records(TopicPartition)}
    // returns unmodifiable Lists, and we want to support adding another batch of 
    // records before all the records in the current batch have been consumed 
    List<List<T>> records;
    int index = 0;
    private volatile boolean paused;
    
    public PartitionBuffer(List<T> records) {
      this.records = new ArrayList<>();
      this.add(records);
    }
    public void add(List<T> records) {
      this.records.add(records);
    }
    
    public void pause() {
      this.paused = true;
    }
    
    public void resume() {
      this.paused = false;
    }
    
    public void flush() {
      this.records.clear();
      this.index = 0;
    }
    
    /**
     * Returns the next buffered record, or null if there is 
     * no next record or if this buffer is paused.
     */
    public T next() {
      if (this.paused) {
        return null;
      }
      if (this.records.isEmpty()) {
        return null;
      }
      List<T> list = this.records.get(0);
      while (this.index == list.size()) {
        this.records.remove(0);
        this.index = 0;
        if (this.records.isEmpty()) {
          return null;
        }
        list = this.records.get(0);
      }
      T result = list.get(this.index++);
      return result;
    }
    
    @Override
    public String toString() {
      return "index: " + this.index + " records: " + this.records;
    }
  }
  
  private final Map<P, PartitionBuffer<R>> map = new ConcurrentHashMap<>();
  private Iterator<P> keyIterator;
  private P currentKey;
  
  public void add(P p, List<R> records) {
    PartitionBuffer<R> list = this.map.get(p);
    if (list == null) {
      this.map.put(p, new PartitionBuffer<>(records));
    } else {
      list.add(records);
    }
  }
  
  public void pause(P p) {
    this.map.get(p).pause();
  }
  
  public void resume(P p) {
    this.map.get(p).resume();
  }
  
  public void flush(P p) {
    this.map.get(p).flush();
  }
  
  /** 
   * Get the next element, or null if there is no next element.
   * This is called from the event loop thread
   */
  public R next() {
    if (this.currentKey == null) {
      this.keyIterator = this.map.keySet().iterator();
      if (!this.keyIterator.hasNext()) {
        return null;
      }
      this.currentKey = this.keyIterator.next();
    }
    P initialKey = this.currentKey;
    R next;
    while ((next = this.map.get(this.currentKey).next()) == null) {
      if (!this.keyIterator.hasNext()) {
        this.keyIterator = this.map.keySet().iterator();
      }
      this.currentKey = this.keyIterator.next();
      if (this.currentKey.equals(initialKey)) {
        break;// return null;
      }
    }
    return next;
  }
  
  @Override
  public String toString() {
    return "currentKey: " + this.currentKey + " map: " + this.map;
  }

  public void flush(Iterable<P> partitions) {
    for (P p : partitions) {
      flush(p);
    }
  }

  public void resume(Iterable<P> partitions) {
    for (P p : partitions) {
      resume(p);
    }
  }
  
  public void pause(Iterable<P> partitions) {
    for (P p : partitions) {
      pause(p);
    }
  }
}
