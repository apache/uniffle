/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.uniffle.common.log;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;
import javax.annotation.concurrent.GuardedBy;

import org.apache.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

public class TestLoggerExtension implements BeforeEachCallback, AfterEachCallback {
  private static final ExtensionContext.Namespace NAMESPACE =
      ExtensionContext.Namespace.create(TestLoggerExtension.class);
  private static final String LOG_COLLECTOR_KEY = "TestLogAppender";
  private TestAppender appender;

  @Override
  public void beforeEach(ExtensionContext context) {
    appender = new TestAppender();
    if (LogManager.getLogger(LogManager.ROOT_LOGGER_NAME) instanceof Logger) {
      org.apache.logging.log4j.core.Logger log4jlogger =
          (org.apache.logging.log4j.core.Logger)
              org.apache.logging.log4j.LogManager.getLogger(LogManager.ROOT_LOGGER_NAME);
      appender.start();
      log4jlogger.addAppender(appender);
    }
    context.getStore(NAMESPACE).put(LOG_COLLECTOR_KEY, this);
  }

  @Override
  public void afterEach(ExtensionContext context) {
    if (LogManager.getLogger(LogManager.ROOT_LOGGER_NAME) instanceof Logger) {
      Logger log4jlogger = (Logger) LogManager.getLogger(LogManager.ROOT_LOGGER_NAME);
      appender.stop();
      log4jlogger.removeAppender(appender);
    }
  }

  public static TestLoggerExtension getTestLogger(ExtensionContext context) {
    return context.getStore(NAMESPACE).get(LOG_COLLECTOR_KEY, TestLoggerExtension.class);
  }

  /**
   * Determine if a specific pattern appears in log output.
   *
   * @param pattern a pattern text to search for in log events
   * @return true if a log message containing the pattern exists, false otherwise
   */
  public boolean wasLogged(String pattern) {
    return appender.wasLogged(Pattern.compile(".*" + pattern + ".*"));
  }

  /**
   * Determine if a specific pattern appears in log output with the specified level.
   *
   * @param pattern a pattern text to search for in log events
   * @return true if a log message containing the pattern exists, false otherwise
   */
  public boolean wasLoggedWithLevel(String pattern, Level level) {
    return appender.wasLoggedWithLevel(Pattern.compile(".*" + pattern + ".*"), level);
  }

  /**
   * Count the number of times a specific pattern appears in log messages.
   *
   * @param pattern Pattern to search for in log events
   * @return The number of log messages which match the pattern
   */
  public int logCount(String pattern) {
    // [\s\S] will match all character include line break
    return appender.logCount(Pattern.compile("[\\s\\S]*" + pattern + "[\\s\\S]*"));
  }

  public class TestAppender extends AbstractAppender {
    @GuardedBy("this") private final List<LogEvent> events = new ArrayList<>();

    protected TestAppender() {
      super("", null, null, false, null);
    }

    /** Determines whether a message with the given pattern was logged. */
    public synchronized boolean wasLogged(Pattern pattern) {
      for (LogEvent e : events) {
        if (pattern.matcher(e.getMessage().getFormattedMessage()).matches()) {
          return true;
        }
      }
      return false;
    }

    /** Determines whether a message with the given pattern was logged. */
    public synchronized boolean wasLoggedWithLevel(Pattern pattern, Level level) {
      for (LogEvent e : events) {
        if (e.getLevel().equals(level)
            && pattern.matcher(e.getMessage().getFormattedMessage()).matches()) {
          return true;
        }
      }
      return false;
    }

    /** Counts the number of log message with a given pattern. */
    public synchronized int logCount(Pattern pattern) {
      int logCount = 0;
      for (LogEvent e : events) {
        if (pattern.matcher(e.getMessage().getFormattedMessage()).matches()) {
          logCount++;
        }
      }
      return logCount;
    }

    @Override
    public synchronized void append(LogEvent event) {
      events.add(event);
    }
  }
}
