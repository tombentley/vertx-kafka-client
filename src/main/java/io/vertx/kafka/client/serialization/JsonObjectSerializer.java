/*
 * Copyright 2016 Red Hat Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.vertx.kafka.client.serialization;

import io.vertx.core.json.JsonObject;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

/**
 * Kafka serializer for raw bytes in a buffer
 */
public class JsonObjectSerializer implements Serializer<JsonObject> {

  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {
  }

  @Override
  public byte[] serialize(String topic, JsonObject data) {
    if (data == null)
      return null;

    return data.encode().getBytes();
  }

  @Override
  public void close() {
  }
}
