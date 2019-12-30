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

package org.apache.hudi.utilities.schema;

import org.apache.hudi.DataSourceUtils;
import org.apache.hudi.common.util.TypedProperties;
import org.apache.hudi.exception.HoodieIOException;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.IOException;
import java.net.URL;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Obtains latest schema from the Confluent/Kafka schema-registry.
 *
 * https://github.com/confluentinc/schema-registry
 */
public class SchemaRegistryProvider extends SchemaProvider {

  /**
   * Configs supported.
   */
  public static class Config {

    private static final String SRC_SCHEMA_REGISTRY_URL_PROP = "hoodie.deltastreamer.schemaprovider.registry.url";
    private static final String TARGET_SCHEMA_REGISTRY_URL_PROP =
        "hoodie.deltastreamer.schemaprovider.registry.targetUrl";
  }

  private final Schema schema;
  private final Schema targetSchema;

  private static String fetchSchemaFromRegistry(String registryUrl) throws IOException {
    URL registry = new URL(registryUrl);
    ObjectMapper mapper = new ObjectMapper();
    JsonNode node = mapper.readTree(registry.openStream());
    return node.get("schema").asText();
  }

  public SchemaRegistryProvider(TypedProperties props, JavaSparkContext jssc) {
    super(props, jssc);
    DataSourceUtils.checkRequiredProperties(props, Collections.singletonList(Config.SRC_SCHEMA_REGISTRY_URL_PROP));
    String registryUrl = props.getString(Config.SRC_SCHEMA_REGISTRY_URL_PROP);
    String targetRegistryUrl = props.getString(Config.TARGET_SCHEMA_REGISTRY_URL_PROP, registryUrl);
    try {
      this.schema = getSchema(registryUrl);
      if (!targetRegistryUrl.equals(registryUrl)) {
        this.targetSchema = getSchema(targetRegistryUrl);
      } else {
        this.targetSchema = schema;
      }
    } catch (IOException ioe) {
      throw new HoodieIOException("Error reading schema from registry :" + registryUrl, ioe);
    }
  }

  private static Schema getSchema(String registryUrl) throws IOException {
    Schema schema = new Schema.Parser().parse(fetchSchemaFromRegistry(registryUrl));
    List<Schema.Field> fields = schema.getFields().stream().map(field -> tansformDecimalSchema(field)).collect(Collectors.toList());
    Schema newSchema = Schema.createRecord(schema.getName(),schema.getDoc(),schema.getNamespace(),false,fields);
    return newSchema;
  }

  // TODO Decimal Type Hard Code
  public static Schema.Field tansformDecimalSchema(Schema.Field field) {
    if (field.schema().getLogicalType() != null && field.schema().getLogicalType().getName() == "decimal") {
      Schema decimalSchema = LogicalTypes.decimal(30,4).addToSchema(Schema.create(Schema.Type.BYTES));
      Schema.Field decimalField = new Schema.Field(field.name(),decimalSchema,field.doc(),field.defaultVal(),field.order());
      return decimalField;
    } else  if (field.schema().getType() == Schema.Type.UNION) {
      if (field.schema().getTypes().get(1).getLogicalType() != null && field.schema().getTypes().get(1).getLogicalType().getName() == "decimal") {
        Schema decimalSchema = LogicalTypes.decimal(30,4).addToSchema(Schema.create(Schema.Type.BYTES));
        Schema unionDecimalSchema = Schema.createUnion(field.schema().getTypes().get(0),decimalSchema);
        Schema.Field unionField = new Schema.Field(field.name(),unionDecimalSchema,field.doc(),field.defaultVal(),field.order());
        return unionField;
      } else {
        Schema.Field newField = new Schema.Field(field.name(),field.schema(),field.doc(),field.defaultVal(),field.order());
        return newField;
      }
    } else {
      Schema.Field newField = new Schema.Field(field.name(),field.schema(),field.doc(),field.defaultVal(),field.order());
      return newField;
    }
  }

  @Override
  public Schema getSourceSchema() {
    return schema;
  }

  @Override
  public Schema getTargetSchema() {
    return targetSchema;
  }
}
