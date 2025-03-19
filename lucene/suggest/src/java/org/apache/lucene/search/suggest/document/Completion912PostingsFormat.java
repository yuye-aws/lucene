/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.lucene.search.suggest.document;

import org.apache.lucene.codecs.PostingsFormat;

/**
 * {@link org.apache.lucene.search.suggest.document.CompletionPostingsFormat} for {@code
 * org.apache.lucene.backward_codecs.lucene912.Lucene912PostingsFormat}. This format is only used
 * for backward-compatibility of the index format and cannot be used to write data, use {@link
 * Completion101PostingsFormat} on new indices.
 *
 * @lucene.experimental
 */
public class Completion912PostingsFormat extends CompletionPostingsFormat {
  /** Creates a {@link Completion912PostingsFormat}. */
  public Completion912PostingsFormat() {
    super("Completion912");
  }

  @Override
  protected PostingsFormat delegatePostingsFormat() {
    return PostingsFormat.forName("Lucene912");
  }
}
