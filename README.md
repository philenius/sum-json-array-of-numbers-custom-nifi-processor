# Custom NiFi Processor: Sum JSON Array of Numbers

This custom NiFi processor sums up all numbers in a given JSON array. The numbers can be of type float, double, long, integer, etc. Internally, this processor uses BigDecimal for parsing the numbers and calculating the sum. Scale and precision of the sum will be based on the number with the greatest precision and or scale so that there's no loss neither in precision nor in scale.

:warning: **Requires JDK 8!**

## Build

```bash
mvn package
```

## Deployment

1. The command `mvn package` will create a `.nar` (NiFi Archive) file in `nifi-sum-json-array-of-numbers-custom-nifi-processor-nar/target/`.

2. Copy the file `nifi-sum-json-array-of-numbers-custom-nifi-processor-nar-1.0.nar` to your NiFi installation dir into the subfolder `$NIFI_HOME/lib/`.

2. Restart NiFi.

