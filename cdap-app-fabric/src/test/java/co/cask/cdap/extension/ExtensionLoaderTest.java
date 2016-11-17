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

package co.cask.cdap.extension;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableSet;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.Map;
import java.util.Set;

/**
 * Tests for {@link ExtensionLoader}
 */
public class ExtensionLoaderTest {
  @ClassRule
  public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

  private static final String GREETING = "hello";
  private static final String LOGGING_KIND = "logging";
  private static final String STDOUT_KIND = "stdout";

  @Test
  public void test() throws IOException {
    String extDirs = TEMPORARY_FOLDER.newFolder().getAbsolutePath();
    ExtensionLoader<String, Extension> loader = new ExtensionLoader<>(extDirs, new Function<Extension, Set<String>>() {
      @Override
      public Set<String> apply(Extension input) {
        ExtensionKind extensionKind = input.getClass().getAnnotation(ExtensionKind.class);
        return ImmutableSet.of(extensionKind.value());
      }
    }, Extension.class, new Extension() {
      @Override
      public String getGreeting() {
        throw new UnsupportedOperationException();
      }

      @Override
      public void echoGreeting() {
        throw new UnsupportedOperationException();
      }
    });
    Map<String, Extension> extensions = loader.listExtensions();
    Assert.assertTrue(extensions.isEmpty());
    loader.preload();
    Assert.assertEquals(2, extensions.size());
    loader.invalidate();
    Assert.assertTrue(loader.listExtensions().isEmpty());
    Extension loggingExtension = loader.getExtension(LOGGING_KIND);
    Assert.assertTrue(loggingExtension instanceof LoggerExtension);
    Assert.assertEquals(generateGreeting(LOGGING_KIND), loggingExtension.getGreeting());
    Extension stdoutExtension = loader.getExtension(STDOUT_KIND);
    Assert.assertTrue(stdoutExtension instanceof StdoutExtension);
    Assert.assertEquals(generateGreeting(STDOUT_KIND), stdoutExtension.getGreeting());
    Extension invalidExtension = loader.getExtension("invalid");
    try {
      invalidExtension.echoGreeting();
      Assert.fail("Should throw UnsupportedOperationException for invalid extensions.");
    } catch (UnsupportedOperationException expected) {
      // expected
    }
  }

  private static String generateGreeting(String kind) {
    return kind + "-" + GREETING;
  }

  private interface Extension {

    String getGreeting();

    void echoGreeting();
  }

  @Retention(RetentionPolicy.RUNTIME)
  @Target(ElementType.TYPE)
  private @interface ExtensionKind {
    String value();
  }

  @ExtensionKind(LOGGING_KIND)
  public static class LoggerExtension implements Extension {

    private static final Logger LOG = LoggerFactory.getLogger(LoggerExtension.class);
    private static final String LOGGING_GREETING = generateGreeting(LOGGING_KIND);

    @Override
    public String getGreeting() {
      return LOGGING_GREETING;
    }

    @Override
    public void echoGreeting() {
      LOG.info(LOGGING_GREETING);
    }
  }

  @ExtensionKind(STDOUT_KIND)
  public static class StdoutExtension implements Extension {

    private static final String STDOUT_GREETING = generateGreeting(STDOUT_KIND);

    @Override
    public String getGreeting() {
      return STDOUT_GREETING;
    }

    @Override
    public void echoGreeting() {
      System.out.println(STDOUT_GREETING);
    }
  }
}
