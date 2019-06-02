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
package de.philenius.nifi;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import com.google.gson.JsonSyntaxException;
import java.math.BigDecimal;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

@Tags({"json", "sum", "aggregate", "float", "integer", "decimal"})
@CapabilityDescription("This processor sums up all numbers in a given JSON array. The numbers "
    + "can be of type float, double, long, integer, etc. Internally, this processor uses "
    + "BigDecimal for parsing the numbers and calculating the sum. Scale and precision of the "
    + "sum will be based on the number with the greatest precision and or scale so that "
    + "there's no loss neither in precision nor in scale.")
@InputRequirement(Requirement.INPUT_REQUIRED)
public class SumJsonArrayOfNumbersProcessor extends AbstractProcessor {

  static final PropertyDescriptor JSON_ARRAY_WITH_NUMBERS =
      new PropertyDescriptor.Builder()
          .name("JSON_ARRAY_WITH_NUMBERS")
          .displayName("JSON array with numbers")
          .description(
              "This property must contain a JSON array of numbers that should be summed up. "
                  + "Alternatively, you can use NiFi expression language to reference an "
                  + "attribute of this FlowFile which contains the actual JSON array.")
          .required(true)
          .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
          .addValidator(StandardValidators.ATTRIBUTE_KEY_VALIDATOR)
          .build();

  static final PropertyDescriptor FLOW_FILE_ATTRIBUTE_FOR_SUM_OUTPUT =
      new PropertyDescriptor.Builder()
          .name("FLOW_FILE_ATTRIBUTE_FOR_SUM_OUTPUT")
          .displayName("FlowFile attribute name for output of sum")
          .description("This property defines the name of the FlowFile attribute "
              + "under which the sum of numbers should be stored at.")
          .defaultValue("sumOfNumbers")
          .expressionLanguageSupported(ExpressionLanguageScope.NONE)
          .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
          .build();

  static final Relationship SUCCESS = new Relationship.Builder().name("SUCCESS")
      .description(
          "FlowFiles are routed to this relationship when the sum was successfully calculated")
      .build();

  static final Relationship JSON_PARSE_FAILURE = new Relationship.Builder()
      .name("JSON_PARSE_FAILURE")
      .description("FlowFiles are routed to this relationship in case of a JSON parsing exception")
      .build();

  static final Relationship NUMBER_PARSE_FAILURE = new Relationship.Builder()
      .name("NUMBER_PARSE_FAILURE")
      .description(
          "FlowFiles are routed to this relationship when parsing the numbers inside the JSON array caused an exception")
      .build();

  static final Relationship FAILURE = new Relationship.Builder().name("FAILURE")
      .description("FlowFiles are routed to this relationship when any exception occurred").build();

  private List<PropertyDescriptor> descriptors;

  private Set<Relationship> relationships;

  private JsonParser jsonParser;

  @Override
  protected void init(final ProcessorInitializationContext context) {
    final List<PropertyDescriptor> descriptors = new ArrayList<>();
    descriptors.add(JSON_ARRAY_WITH_NUMBERS);
    descriptors.add(FLOW_FILE_ATTRIBUTE_FOR_SUM_OUTPUT);
    this.descriptors = Collections.unmodifiableList(descriptors);

    final Set<Relationship> relationships = new HashSet<>();
    relationships.add(SUCCESS);
    relationships.add(JSON_PARSE_FAILURE);
    relationships.add(NUMBER_PARSE_FAILURE);
    relationships.add(FAILURE);
    this.relationships = Collections.unmodifiableSet(relationships);

    this.jsonParser = new JsonParser();
  }

  @Override
  public Set<Relationship> getRelationships() {
    return this.relationships;
  }

  @Override
  public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
    return descriptors;
  }

  @OnScheduled
  public void onScheduled(final ProcessContext context) {

  }

  @Override
  public void onTrigger(final ProcessContext context, final ProcessSession session)
      throws ProcessException {

    FlowFile flowFile = session.get();

    if (flowFile == null) {
      return;
    }

    String jsonArrayOfNumbers = context
        .getProperty(JSON_ARRAY_WITH_NUMBERS)
        .evaluateAttributeExpressions(flowFile)
        .getValue();

    if (jsonArrayOfNumbers == null || jsonArrayOfNumbers.isEmpty()) {
      getLogger().error(
          "Resolved NiFi expression language to 'null' or empty string. Cannot parse 'null' array. "
              + "The referenced FlowFile attribute might not exist or the expression language syntax might be incorrect.");
      session.transfer(flowFile, FAILURE);
      return;
    }

    try {

      JsonArray numbers = jsonParser.parse(jsonArrayOfNumbers).getAsJsonArray();
      BigDecimal sum = BigDecimal.ZERO;
      for (JsonElement e : numbers) {
        BigDecimal bigDecimalValue = parseNumberToBigDecimal(e.getAsString());
        sum = sum.add(bigDecimalValue);
      }
      session.putAttribute(flowFile,
          context.getProperty(FLOW_FILE_ATTRIBUTE_FOR_SUM_OUTPUT).getValue(),
          sum.toString());

    } catch (JsonSyntaxException | IllegalStateException e) {

      getLogger().error(e.getMessage(), e);
      session.transfer(flowFile, JSON_PARSE_FAILURE);
      return;

    } catch (ParseException e) {

      getLogger().error(e.getMessage(), e);
      session.transfer(flowFile, NUMBER_PARSE_FAILURE);
      return;

    } catch (Exception e) {

      getLogger().error(e.getMessage(), e);
      session.transfer(flowFile, FAILURE);
      return;

    }

    session.transfer(flowFile, SUCCESS);

  }

  protected static BigDecimal parseNumberToBigDecimal(String number) throws ParseException {

    DecimalFormat decimalFormat = (DecimalFormat) NumberFormat.getInstance(Locale.ENGLISH);
    decimalFormat.setParseBigDecimal(true);

    // replacing all commas by points is necessary when the NiFi processor / NiFi is executed with a non-english locale
    String numberWithDot = number.replaceAll(",", ".");
    return (BigDecimal) decimalFormat.parseObject(numberWithDot);

  }
}
