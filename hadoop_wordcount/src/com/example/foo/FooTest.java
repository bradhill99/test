package com.example.foo;

import static org.junit.Assert.assertEquals;

import java.io.IOException;

import org.apache.pig.pigunit.PigTest;
import org.apache.pig.tools.parameters.ParseException;
import org.junit.Test;
import org.junit.Ignore;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;


@RunWith(JUnit4.class)
public class FooTest {

    @Test
    public void thisAlwaysPasses() {
    }

    @Test
    @Ignore
    public void thisIsIgnored() {
    }
    @Test
    public void testTop2Queries() throws IOException, ParseException {
      String[] args = {
          "n=2",
          };
   
      PigTest test = new PigTest("top_queries.pig", args);
   
      String[] input = {
          "yahoo",
          "yahoo",
          "yahoo",
          "twitter",
          "facebook",
          "facebook",
          "linkedin",
      };
   
      String[] output = {
          "(yahoo,3)",
          "(facebook,2)",
      };
   
      test.assertOutput("data", input, "queries_limit", output);
    }
}