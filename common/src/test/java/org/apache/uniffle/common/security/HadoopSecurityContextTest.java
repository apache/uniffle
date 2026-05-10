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

package org.apache.uniffle.common.security;

import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledIfSystemProperty;

import org.apache.uniffle.common.KerberizedHadoopBase;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

@DisabledIfSystemProperty(
    named = "uniffle.test.skip.kerberos",
    matches = "true",
    disabledReason = "MiniKdc is not compatible with JDK 17+")
public class HadoopSecurityContextTest extends KerberizedHadoopBase {

  @BeforeAll
  public static void beforeAll() throws Exception {
    testRunner = HadoopSecurityContextTest.class;
    KerberizedHadoopBase.init();
  }

  @Test
  public void testSecuredCallable() throws Exception {
    try (HadoopSecurityContext context =
        new HadoopSecurityContext(
            null, kerberizedHadoop.getHdfsKeytab(), kerberizedHadoop.getHdfsPrincipal(), 1000)) {

      // case1: when user is empty or null, it should throw exception
      try {
        context.runSecured(StringUtils.EMPTY, (Callable<Void>) () -> null);
        fail();
      } catch (Exception e) {
        return;
      }

      // case2: run by the login user, there is no need to wrap proxy action
      Path pathWithHdfsUser = new Path("/hdfs/HadoopSecurityContextTest");
      context.runSecured(
          "hdfs",
          (Callable<Void>)
              () -> {
                kerberizedHadoop.getFileSystem().mkdirs(pathWithHdfsUser);
                return null;
              });
      FileStatus fileStatus = kerberizedHadoop.getFileSystem().getFileStatus(pathWithHdfsUser);
      assertEquals("hdfs", fileStatus.getOwner());

      // case3: run by the proxy user
      Path pathWithAlexUser = new Path("/alex/HadoopSecurityContextTest");
      AtomicReference<UserGroupInformation> ugi1 = new AtomicReference<>();
      context.runSecured(
          "alex",
          (Callable<Void>)
              () -> {
                ugi1.set(UserGroupInformation.getCurrentUser());
                kerberizedHadoop.getFileSystem().mkdirs(pathWithAlexUser);
                return null;
              });
      fileStatus = kerberizedHadoop.getFileSystem().getFileStatus(pathWithAlexUser);
      assertEquals("alex", fileStatus.getOwner());

      // case4: run by the proxy user again, it will always return the same
      // ugi and filesystem instance.
      AtomicReference<UserGroupInformation> ugi2 = new AtomicReference<>();
      context.runSecured(
          "alex",
          (Callable<Void>)
              () -> {
                ugi2.set(UserGroupInformation.getCurrentUser());
                return null;
              });
      assertTrue(ugi1.get() == ugi2.get());
      assertTrue(ugi1.get() == context.getProxyUserUgiPool().get("alex"));

      FileSystem fileSystem1 =
          context.runSecured("alex", () -> FileSystem.get(kerberizedHadoop.getConf()));
      FileSystem fileSystem2 =
          context.runSecured("alex", () -> FileSystem.get(kerberizedHadoop.getConf()));
      assertTrue(fileSystem1 == fileSystem2);
    }
  }

  @Test
  public void testSecuredDisableProxyUser() throws Exception {
    try (HadoopSecurityContext context =
        new HadoopSecurityContext(
            null,
            kerberizedHadoop.getHdfsKeytab(),
            kerberizedHadoop.getHdfsPrincipal(),
            1000,
            false)) {
      Path pathWithHdfsUser = new Path("/alex/HadoopSecurityDisableProxyUser");
      context.runSecured(
          "alex",
          (Callable<Void>)
              () -> {
                kerberizedHadoop.getFileSystem().mkdirs(pathWithHdfsUser);
                return null;
              });
      FileStatus fileStatus = kerberizedHadoop.getFileSystem().getFileStatus(pathWithHdfsUser);
      assertEquals("hdfs", fileStatus.getOwner());
    }
  }

  @Test
  public void testCreateIllegalContext() throws Exception {
    System.setProperty("sun.security.krb5.debug", "true");

    // case1: lack principal, should throw exception
    try (HadoopSecurityContext context =
        new HadoopSecurityContext(null, kerberizedHadoop.getHdfsKeytab(), null, 1000)) {
      fail();
    } catch (Exception e) {
      assertTrue(e.getMessage().contains("principal must be not null or empty"));
    }

    // case2: lack keytab, should throw exception
    try (HadoopSecurityContext context =
        new HadoopSecurityContext(null, null, kerberizedHadoop.getHdfsPrincipal(), 1000)) {
      fail();
    } catch (Exception e) {
      assertTrue(e.getMessage().contains("KeytabFilePath must be not null or empty"));
    }

    // case3: illegal re-login interval sec
    try (HadoopSecurityContext context =
        new HadoopSecurityContext(
            null, kerberizedHadoop.getHdfsKeytab(), kerberizedHadoop.getHdfsPrincipal(), 0)) {
      fail();
    } catch (Exception e) {
      assertTrue(e.getMessage().contains("refreshIntervalSec must be not negative"));
    }

    // case4: After setting the krb5 conf, it should pass
    String krbConfFilePath = System.getProperty("java.security.krb5.conf");
    System.clearProperty("java.security.krb5.conf");
    HadoopSecurityContext context =
        new HadoopSecurityContext(
            krbConfFilePath,
            kerberizedHadoop.getHdfsKeytab(),
            kerberizedHadoop.getHdfsPrincipal(),
            100);
    context.close();

    // recover System property of krb5 conf
    System.setProperty("java.security.krb5.conf", krbConfFilePath);
  }

  @Test
  public void testWithOutKrb5Conf() {
    // case: lack krb5 conf, should throw exception
    String krbConfFilePath = System.getProperty("java.security.krb5.conf");
    System.clearProperty("java.security.krb5.conf");
    try (HadoopSecurityContext context2 =
        new HadoopSecurityContext(
            null, kerberizedHadoop.getHdfsKeytab(), kerberizedHadoop.getHdfsPrincipal(), 100)) {
      fail();
    } catch (Exception e) {
      assertTrue(e.getMessage().contains("Cannot locate KDC"));
    }

    // recover System property of krb5 conf
    System.setProperty("java.security.krb5.conf", krbConfFilePath);
  }
}
