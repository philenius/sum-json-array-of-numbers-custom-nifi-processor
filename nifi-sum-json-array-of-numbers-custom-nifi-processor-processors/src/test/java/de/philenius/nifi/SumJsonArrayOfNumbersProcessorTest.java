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

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class SumJsonArrayOfNumbersProcessorTest {

  private TestRunner testRunner;
  private String sumOutputAttribute = "total";

  @BeforeEach
  public void init() {

    testRunner = TestRunners.newTestRunner(SumJsonArrayOfNumbersProcessor.class);

    testRunner.setProperty(SumJsonArrayOfNumbersProcessor.JSON_ARRAY_WITH_NUMBERS,
        "${jsonArray}");
    testRunner.setProperty(SumJsonArrayOfNumbersProcessor.FLOW_FILE_ATTRIBUTE_FOR_SUM_OUTPUT,
        sumOutputAttribute);

  }

  @Test
  public void testSuccessfulProcessingSetJsonArrayDirectlyWithoutExpressionLanguage() {

    final BigDecimal expectedSum = new BigDecimal("50.00");

    testRunner.setProperty(SumJsonArrayOfNumbersProcessor.JSON_ARRAY_WITH_NUMBERS, "[ 34.99, 15.01 ]");
    testRunner.enqueue("{}");
    testRunner.run();

    testRunner.assertAllFlowFiles(flowFile -> {
      final BigDecimal actualSum = new BigDecimal(flowFile.getAttribute(sumOutputAttribute));
      Assertions.assertEquals(expectedSum, actualSum);
    });
    testRunner.assertAllFlowFilesTransferred(SumJsonArrayOfNumbersProcessor.SUCCESS);

  }

  @Test
  public void testSuccessfulProcessing() {

    Map<String, String> attributes = new HashMap<>();
    attributes.put("jsonArray", "[ 34.99, 15.01 ]");
    final BigDecimal expectedSum = new BigDecimal("50.00");

    testRunner.enqueue("{}", attributes);
    testRunner.run();

    testRunner.assertAllFlowFiles(flowFile -> {
      final BigDecimal actualSum = new BigDecimal(flowFile.getAttribute(sumOutputAttribute));
      Assertions.assertEquals(expectedSum, actualSum);
    });
    testRunner.assertAllFlowFilesTransferred(SumJsonArrayOfNumbersProcessor.SUCCESS);

  }

  @Test
  public void testSuccessfulProcessingWithNoValues() {

    Map<String, String> attributes = new HashMap<>();
    attributes.put("jsonArray", "[]");
    final BigDecimal expectedSum = BigDecimal.ZERO;

    testRunner.enqueue("{}", attributes);
    testRunner.run();

    testRunner.assertAllFlowFiles(flowFile -> {
      final BigDecimal actualSum = new BigDecimal(flowFile.getAttribute(sumOutputAttribute));
      Assertions.assertEquals(expectedSum, actualSum);
    });
    testRunner.assertAllFlowFilesTransferred(SumJsonArrayOfNumbersProcessor.SUCCESS);

  }

  @Test
  public void testSuccessfulProcessingWithManyValues() {

    Map<String, String> attributes = new HashMap<>();
    attributes.put("jsonArray", "[ 21.45, 89.99, 40.00, 74.01, 888.00 ]");
    final BigDecimal expectedSum = new BigDecimal("1113.45");

    testRunner.enqueue("{}", attributes);
    testRunner.run();

    testRunner.assertAllFlowFiles(flowFile -> {
      final BigDecimal actualSum = new BigDecimal(flowFile.getAttribute(sumOutputAttribute));
      Assertions.assertEquals(expectedSum, actualSum);
    });
    testRunner.assertAllFlowFilesTransferred(SumJsonArrayOfNumbersProcessor.SUCCESS);

  }

  @Test
  public void testSuccessfulProcessingWithSingleValue() {

    Map<String, String> attributes = new HashMap<>();
    attributes.put("jsonArray", "[ 21.45 ]");
    final BigDecimal expectedSum = new BigDecimal("21.45");

    testRunner.enqueue("{}", attributes);
    testRunner.run();

    testRunner.assertAllFlowFiles(flowFile -> {
      final BigDecimal actualSum = new BigDecimal(flowFile.getAttribute(sumOutputAttribute));
      Assertions.assertEquals(expectedSum, actualSum);
    });
    testRunner.assertAllFlowFilesTransferred(SumJsonArrayOfNumbersProcessor.SUCCESS);

  }

  @Test
  public void testSuccessfulProcessingWithZeroScale() {

    Map<String, String> attributes = new HashMap<>();
    attributes.put("jsonArray", "[ 10, 5 ]");
    final BigDecimal expectedSum = new BigDecimal("15");

    testRunner.enqueue("{}", attributes);
    testRunner.run();

    testRunner.assertAllFlowFiles(flowFile -> {
      final BigDecimal actualSum = new BigDecimal(flowFile.getAttribute(sumOutputAttribute));
      Assertions.assertEquals(expectedSum, actualSum);
    });
    testRunner.assertAllFlowFilesTransferred(SumJsonArrayOfNumbersProcessor.SUCCESS);

  }

  @Test
  public void testSuccessfulProcessingWithGreaterScale() {

    Map<String, String> attributes = new HashMap<>();
    attributes.put("jsonArray", "[ 10.4557654, 5.00 ]");
    final BigDecimal expectedSum = new BigDecimal("15.4557654");

    testRunner.enqueue("{}", attributes);
    testRunner.run();

    testRunner.assertAllFlowFiles(flowFile -> {
      final BigDecimal actualSum = new BigDecimal(flowFile.getAttribute(sumOutputAttribute));
      Assertions.assertEquals(expectedSum, actualSum);
    });
    testRunner.assertAllFlowFilesTransferred(SumJsonArrayOfNumbersProcessor.SUCCESS);

  }

  @Test
  public void testSetFlowFileSumOutputAttribute() {

    final String sumOutputAttribute = "mySum";
    testRunner.setProperty(SumJsonArrayOfNumbersProcessor.FLOW_FILE_ATTRIBUTE_FOR_SUM_OUTPUT,
        sumOutputAttribute);

    Map<String, String> attributes = new HashMap<>();
    attributes.put("jsonArray", "[ 20, 5.05 ]");

    final BigDecimal expectedSum = new BigDecimal("25.05");

    testRunner.enqueue("{}", attributes);
    testRunner.run();

    testRunner.assertAllFlowFiles(flowFile -> {
      final BigDecimal actualSum = new BigDecimal(flowFile.getAttribute(sumOutputAttribute));
      Assertions.assertEquals(expectedSum, actualSum);
    });
    testRunner.assertAllFlowFilesTransferred(SumJsonArrayOfNumbersProcessor.SUCCESS);

  }

  @Test
  public void testFailedProcessingWhenExpressionLanguageEvaluatesToNull() {

    testRunner.setProperty(SumJsonArrayOfNumbersProcessor.JSON_ARRAY_WITH_NUMBERS,
        "${notExistingAttribute}");

    Map<String, String> attributes = new HashMap<>();
    attributes.put("jsonArray", "[ 20, 5.05 ]");

    testRunner.enqueue("{}", attributes);
    testRunner.run();

    testRunner.assertAllFlowFilesTransferred(SumJsonArrayOfNumbersProcessor.FAILURE);

  }

  @Test
  public void testFailedProcessingWithJsonParseFailure() {

    Map<String, String> attributes = new HashMap<>();
    attributes.put("jsonArray", "{}");

    testRunner.enqueue("{}", attributes);
    testRunner.run();

    testRunner.assertAllFlowFilesTransferred(SumJsonArrayOfNumbersProcessor.JSON_PARSE_FAILURE);

  }

  @Test
  public void testFailedProcessingWhenArrayContainsStrings () {

    Map<String, String> attributes = new HashMap<>();
    attributes.put("jsonArray", "[ \"hello\", 15.99 ]");

    testRunner.enqueue("{}", attributes);
    testRunner.run();

    testRunner.assertAllFlowFilesTransferred(SumJsonArrayOfNumbersProcessor.NUMBER_PARSE_FAILURE);

  }

  @Test
  public void testParseNumberToBigDecimal() {

    Assertions.assertDoesNotThrow(() -> {
      final BigDecimal actual = SumJsonArrayOfNumbersProcessor.parseNumberToBigDecimal("56.77");
      final BigDecimal expected = new BigDecimal("56.77");
      Assertions.assertEquals(expected, actual);
    });

    Assertions.assertDoesNotThrow(() -> {
      final BigDecimal actual = SumJsonArrayOfNumbersProcessor.parseNumberToBigDecimal("0.00");
      Assertions.assertEquals(BigDecimal.ZERO, actual.setScale(0));
    });

    Assertions.assertDoesNotThrow(() -> {
      final BigDecimal actual = SumJsonArrayOfNumbersProcessor.parseNumberToBigDecimal("-15.49");
      final BigDecimal expected = new BigDecimal("-15.49");
      Assertions.assertEquals(expected, actual);
    });

    Assertions.assertDoesNotThrow(() -> {
      final BigDecimal actual = SumJsonArrayOfNumbersProcessor.parseNumberToBigDecimal("78,99");
      final BigDecimal expected = new BigDecimal("78.99");
      Assertions.assertEquals(expected, actual);
    });

    Assertions.assertDoesNotThrow(() -> {
      final BigDecimal actual = SumJsonArrayOfNumbersProcessor.parseNumberToBigDecimal("00,0");
      Assertions.assertEquals(BigDecimal.ZERO, actual.setScale(0));
    });

    Assertions.assertDoesNotThrow(() -> {
      final BigDecimal actual = SumJsonArrayOfNumbersProcessor.parseNumberToBigDecimal("-78,20");
      final BigDecimal expected = new BigDecimal("-78.20");
      Assertions.assertEquals(expected, actual);
    });

  }

}
