/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.giraph.examples;


import org.apache.giraph.BspCase;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.edge.ByteArrayEdges;
import org.apache.giraph.examples.LinkRank.LinkRankComputation;
import org.apache.giraph.examples.LinkRank.LinkRankVertexMasterCompute;
import org.apache.giraph.examples.LinkRank.NutchTableEdgeInputFormat;
import org.apache.giraph.examples.LinkRank.NutchTableEdgeOutputFormat;
import org.apache.giraph.job.GiraphJob;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.MiniZooKeeperCluster;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.UUID;

import static org.junit.Assert.*;

/**
 * Test case for LinkRank reading edges and vertex scores
 * from HBase, calculates new scores and updates the HBase
 * Table again.
 */
public class LinkRankHBaseTest extends BspCase {


  private final Logger LOG = Logger.getLogger(LinkRankHBaseTest.class);

  private static final String TABLE_NAME = "simple_graph";
  private static final double DELTA = 1e-3;

  private HBaseTestingUtility testUtil;
  private Path hbaseRootdir;


  public LinkRankHBaseTest() {
    super(LinkRankHBaseTest.class.getName());

    // Let's set up the hbase root directory.
    Configuration conf = HBaseConfiguration.create();
    try {
      FileSystem fs = FileSystem.get(conf);
      String randomStr = UUID.randomUUID().toString();
      String tmpdir = System.getProperty("java.io.tmpdir") + "/" +
          randomStr + "/";
      hbaseRootdir = fs.makeQualified(new Path(tmpdir));
      conf.set(HConstants.HBASE_DIR, hbaseRootdir.toString());
      fs.mkdirs(hbaseRootdir);
    } catch(IOException ioe) {
      fail("Could not create hbase root directory.");
    }

    // Start the test utility.
    testUtil = new HBaseTestingUtility(conf);
  }

  @Test
  public void testHBaseInputOutput() throws Exception {

    if (System.getProperty("prop.mapred.job.tracker") != null) {
      if(LOG.isInfoEnabled())
        LOG.info("testHBaseInputOutput: Ignore this test if not local mode.");
      return;
    }

    File jarTest = new File(System.getProperty("prop.jarLocation"));
    if(!jarTest.exists()) {
      fail("Could not find Giraph jar at " +
          "location specified by 'prop.jarLocation'. " +
          "Make sure you built the main Giraph artifact?.");
    }

    MiniHBaseCluster cluster = null;
    MiniZooKeeperCluster zkCluster = null;
    FileSystem fs = null;

    try {
      // using the restart method allows us to avoid having the hbase
      // root directory overwritten by /home/$username
      zkCluster = testUtil.startMiniZKCluster();
      testUtil.restartHBaseCluster(2);
      cluster = testUtil.getMiniHBaseCluster();

      final byte[] FAM_OL = Bytes.toBytes("ol");
      final byte[] FAM_S = Bytes.toBytes("s");
      final byte[] TAB = Bytes.toBytes(TABLE_NAME);

      Configuration conf = cluster.getConfiguration();
      HTableDescriptor desc = new HTableDescriptor(TAB);
      desc.addFamily(new HColumnDescriptor(FAM_OL));
      desc.addFamily(new HColumnDescriptor(FAM_S));
      HBaseAdmin hbaseAdmin=new HBaseAdmin(conf);
      if (hbaseAdmin.isTableAvailable(TABLE_NAME)) {
        hbaseAdmin.disableTable(TABLE_NAME);
        hbaseAdmin.deleteTable(TABLE_NAME);
      }
      hbaseAdmin.createTable(desc);

      /**
       * Enter the initial data
       * (a,b), (b,c), (a,c)
       * a = 0.33
       * b = 0.33
       * c = 0.33
       */

      HTable table = new HTable(conf, TABLE_NAME);
      Put p1 = new Put(Bytes.toBytes("a"));
      //ol:b
      p1.add(Bytes.toBytes("ol"), Bytes.toBytes("b"), Bytes.toBytes("ab"));
      //s:S
      p1.add(Bytes.toBytes("s"), Bytes.toBytes("s"), Bytes.toBytes(0.33d));

      Put p2 = new Put(Bytes.toBytes("a"));
      p2.add(Bytes.toBytes("ol"), Bytes.toBytes("c"), Bytes.toBytes("ac"));

      Put p3 = new Put(Bytes.toBytes("b"));
      p3.add(Bytes.toBytes("ol"), Bytes.toBytes("c"), Bytes.toBytes("bc"));
      p3.add(Bytes.toBytes("s"), Bytes.toBytes("s"), Bytes.toBytes(0.33d));

      Put p4 = new Put(Bytes.toBytes("c"));
      p4.add(Bytes.toBytes("s"), Bytes.toBytes("s"), Bytes.toBytes(0.33d));

      table.put(p1);
      table.put(p2);
      table.put(p3);
      table.put(p4);


      // Set Giraph configuration
      //now operate over HBase using Vertex I/O formats
      conf.set(TableInputFormat.INPUT_TABLE, TABLE_NAME);
      conf.set(TableOutputFormat.OUTPUT_TABLE, TABLE_NAME);

      // Start the giraph job
      GiraphJob giraphJob = new GiraphJob(conf, BspCase.getCallingMethodName());
      GiraphConfiguration giraphConf = giraphJob.getConfiguration();
      giraphConf.setZooKeeperConfiguration(
          cluster.getMaster().getZooKeeper().getQuorum());
      setupConfiguration(giraphJob);
      giraphConf.setComputationClass(LinkRankComputation.class);
      giraphConf.setMasterComputeClass(LinkRankVertexMasterCompute.class);
      giraphConf.setOutEdgesClass(ByteArrayEdges.class);
      giraphConf.setVertexInputFormatClass(NutchTableEdgeInputFormat.class);
      giraphConf.setVertexOutputFormatClass(NutchTableEdgeOutputFormat.class);
      giraphConf.setInt("giraph.pageRank.superstepCount", 40);

      assertTrue(giraphJob.run(true));

      if(LOG.isInfoEnabled())
        LOG.info("Giraph job successful. Checking output qualifier.");

      /** Check the results **/

      Result result;
      String key, value;
      byte[] calculatedScoreByte;
      HashMap actualValues = new HashMap<String, Double>();
      actualValues.put("a", 0.19757964896759084d);
      actualValues.put("b", 0.28155100077907663d);
      actualValues.put("c", 0.5208693502533326d);

      for (Object keyobj : actualValues.keySet()){
        key = keyobj.toString();
        result = table.get(new Get(key.getBytes()));
        calculatedScoreByte = result.getValue(FAM_S, FAM_S);
        assertNotNull(calculatedScoreByte);
        assertTrue(calculatedScoreByte.length > 0);
        Assert.assertEquals("scores are not the same", (Double)actualValues.get(key), Bytes.toDouble(calculatedScoreByte), DELTA);
      }
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
      if (zkCluster != null) {
        zkCluster.shutdown();
      }
      // clean test files
      if (fs != null) {
        fs.delete(hbaseRootdir);
      }
    }
  }
}
