/*
 * Copyright © 2016 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.cdap.messaging.store;

import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.TopicId;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Table to store information about the topics and their properties.
 */
public interface MetadataTable extends Closeable {

  /**
   * Create Metadata Table.
   *
   * @throws IOException
   */
  void createTableIfNotExists() throws IOException;

  /**
   * Fetch the properties of the {@link TopicId}.
   *
   * @param topicId message topic
   * @return properties of the {@link TopicId} or null if the topic is not present
   */
  @Nullable
  Map<String, String> getProperties(TopicId topicId) throws IOException;

  /**
   * Create a topic with properties.
   *
   * @param topicId message topic
   * @param properties properties of the {@link TopicId}
   */
  void createTopic(TopicId topicId, @Nullable Map<String, String> properties) throws IOException;

  /**
   * Delete a topic.
   *
   * @param topicId message topic
   */
  void deleteTopic(TopicId topicId) throws IOException;

  /**
   * List all the topics in a namespace.
   *
   * @param namespaceId namespace
   * @return {@link List} of topics in that namespace
   */
  List<TopicId> listTopics(NamespaceId namespaceId) throws IOException;
}
