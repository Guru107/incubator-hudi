/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi;

import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.util.TypedProperties;
import org.apache.hudi.exception.HoodieException;

import org.apache.avro.generic.GenericRecord;

import java.util.Arrays;
import java.util.List;

/**
 * Simple Key generator for unpartitioned Hive Tables.
 */
public class NonPartitionedKeyGenerator extends SimpleKeyGenerator {

  private static final String EMPTY_PARTITION = "";
  protected final List<String> recordKeyFields;

  public NonPartitionedKeyGenerator(TypedProperties props) {
    super(props);
    this.recordKeyFields = Arrays.asList(props.getString(DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY()).split(","));
  }

  @Override
  public HoodieKey getKey(GenericRecord record) {
    if (recordKeyFields == null) {
      throw new HoodieException("Unable to find field names for record key or partition path in cfg");
    }
    StringBuilder recordKey = new StringBuilder();
    for (String recordKeyField : recordKeyFields) {
      recordKey.append(recordKeyField + ":" + DataSourceUtils.getNestedFieldValAsString(record, recordKeyField) + ",");
    }
    recordKey.deleteCharAt(recordKey.length() - 1);
    return new HoodieKey(recordKey.toString(), EMPTY_PARTITION);
  }
}
