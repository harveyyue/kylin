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

package org.apache.kylin.engine.spark;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayPrimitiveWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.AbstractApplication;
import org.apache.kylin.common.util.Bytes;
import org.apache.kylin.common.util.BytesUtil;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.common.util.OptionsHelper;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.engine.mr.common.AbstractHadoopJob;
import org.apache.kylin.engine.mr.common.BatchConstants;
import org.apache.kylin.engine.mr.common.SerializableConfiguration;
import org.apache.kylin.engine.mr.steps.FactDistinctColumnsBase;
import org.apache.kylin.engine.mr.steps.FactDistinctColumnsReducerMapping;
import org.apache.kylin.engine.mr.steps.SelfDefineSortableKey;
import org.apache.kylin.job.JoinedFlatTable;
import org.apache.kylin.job.constant.ExecutableConstants;
import org.apache.spark.Partitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.util.LongAccumulator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;
import scala.Tuple3;

import java.io.IOException;
import java.io.Serializable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class SparkFactDistinct extends AbstractApplication implements Serializable {

    protected static final Logger logger = LoggerFactory.getLogger(SparkFactDistinct.class);

    public static final Option OPTION_CUBE_NAME = OptionBuilder.withArgName(BatchConstants.ARG_CUBE_NAME).hasArg()
            .isRequired(true).withDescription("Cube Name").create(BatchConstants.ARG_CUBE_NAME);
    public static final Option OPTION_META_URL = OptionBuilder.withArgName("metaUrl").hasArg().isRequired(true)
            .withDescription("HDFS metadata url").create("metaUrl");
    public static final Option OPTION_OUTPUT_PATH = OptionBuilder.withArgName(BatchConstants.ARG_OUTPUT).hasArg()
            .isRequired(true).withDescription("Cube output path").create(BatchConstants.ARG_OUTPUT);
    public static final Option OPTION_SEGMENT_ID = OptionBuilder.withArgName("segmentId").hasArg().isRequired(true)
            .withDescription("Cube Segment Id").create("segmentId");
    public static final Option OPTION_STATS_SAMPLING_PERCENT = OptionBuilder
            .withArgName(BatchConstants.ARG_STATS_SAMPLING_PERCENT).hasArg().isRequired(true)
            .withDescription("Statistics sampling percent").create(BatchConstants.ARG_STATS_SAMPLING_PERCENT);
    public static final Option OPTION_INPUT_TABLE = OptionBuilder.withArgName("hiveTable").hasArg().isRequired(true)
            .withDescription("Hive Intermediate Table").create("hiveTable");
    public static final Option OPTION_INPUT_PATH = OptionBuilder.withArgName(BatchConstants.ARG_INPUT).hasArg()
            .isRequired(true).withDescription("Hive Intermediate Table PATH").create(BatchConstants.ARG_INPUT);
    public static final Option OPTION_COUNTER_PATH = OptionBuilder.withArgName(BatchConstants.ARG_COUNTER_OUTPUT)
            .hasArg().isRequired(true).withDescription("counter output path").create(BatchConstants.ARG_COUNTER_OUTPUT);

    private Options options;

    public SparkFactDistinct() {
        options = new Options();
        options.addOption(OPTION_CUBE_NAME);
        options.addOption(OPTION_META_URL);
        options.addOption(OPTION_OUTPUT_PATH);
        options.addOption(OPTION_INPUT_TABLE);
        options.addOption(OPTION_INPUT_PATH);
        options.addOption(OPTION_SEGMENT_ID);
        options.addOption(OPTION_STATS_SAMPLING_PERCENT);
        options.addOption(OPTION_COUNTER_PATH);
    }

    @Override
    protected Options getOptions() {
        return options;
    }

    @Override
    protected void execute(OptionsHelper optionsHelper) throws Exception {
        String cubeName = optionsHelper.getOptionValue(OPTION_CUBE_NAME);
        String metaUrl = optionsHelper.getOptionValue(OPTION_META_URL);
        String segmentId = optionsHelper.getOptionValue(OPTION_SEGMENT_ID);
        String hiveTable = optionsHelper.getOptionValue(OPTION_INPUT_TABLE);
        String inputPath = optionsHelper.getOptionValue(OPTION_INPUT_PATH);
        String outputPath = optionsHelper.getOptionValue(OPTION_OUTPUT_PATH);
        String counterPath = optionsHelper.getOptionValue(OPTION_COUNTER_PATH);
        int samplingPercent = Integer.parseInt(optionsHelper.getOptionValue(OPTION_STATS_SAMPLING_PERCENT));

        Class[] kryoClassArray = new Class[]{Class.forName("scala.reflect.ClassTag$$anon$1"),
                Class.forName("org.apache.kylin.engine.mr.steps.SelfDefineSortableKey")};

        SparkConf conf = new SparkConf().setAppName("Fact distinct columns for:" + cubeName + " segment " + segmentId);
        //serialization conf
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        conf.set("spark.kryo.registrator", "org.apache.kylin.engine.spark.KylinKryoRegistrator");
        conf.set("spark.kryo.registrationRequired", "true").registerKryoClasses(kryoClassArray);

        KylinSparkJobListener jobListener = new KylinSparkJobListener();
        try (JavaSparkContext sc = new JavaSparkContext(conf)) {
            sc.sc().addSparkListener(jobListener);
            HadoopUtil.deletePath(sc.hadoopConfiguration(), new Path(outputPath));

            final SerializableConfiguration sConf = new SerializableConfiguration(sc.hadoopConfiguration());
            KylinConfig envConfig = AbstractHadoopJob.loadKylinConfigFromHdfs(sConf, metaUrl);

            final CubeInstance cubeInstance = CubeManager.getInstance(envConfig).getCube(cubeName);

            final Job job = Job.getInstance(sConf.get());

            final FactDistinctColumnsReducerMapping reducerMapping = new FactDistinctColumnsReducerMapping(
                    cubeInstance);

            logger.info("RDD Output path: {}", outputPath);
            logger.info("getTotalReducerNum: {}", reducerMapping.getTotalReducerNum());
            logger.info("getCuboidRowCounterReducerNum: {}", reducerMapping.getCuboidRowCounterReducerNum());
            logger.info("counter path {}", counterPath);

            boolean isSequenceFile = JoinedFlatTable.SEQUENCEFILE
                    .equalsIgnoreCase(envConfig.getFlatTableStorageFormat());
            StorageLevel storageLevel = StorageLevel.fromString(envConfig.getSparkStorageLevel());

            // calculate source record bytes size
            final LongAccumulator bytesWritten = sc.sc().longAccumulator();

            final JavaRDD<String[]> recordRDD = SparkUtil.hiveRecordInputRDD(isSequenceFile, sc, inputPath, hiveTable);

            // read record from flat table
            // output:
            //   1, statistic
            //   2, field value of dict col
            //   3, min/max field value of not dict col
            JavaPairRDD<SelfDefineSortableKey, Text> flatOutputRDD = recordRDD.mapPartitionsToPair(
                    new FlatOutputFunction(cubeName, segmentId, metaUrl, sConf, samplingPercent, bytesWritten))
                    .persist(storageLevel);

            // repartition data, make each reducer handle only one col data or the statistic data
            JavaPairRDD<SelfDefineSortableKey, Text> aggredRDD = flatOutputRDD.repartitionAndSortWithinPartitions(
                    new FactDistinctPartitioner(cubeName, metaUrl, sConf, reducerMapping.getTotalReducerNum()));

            // multiple output result
            // 1, CFG_OUTPUT_COLUMN: field values of dict col, which will not be built in reducer, like globalDictCol
            // 2, CFG_OUTPUT_DICT: dictionary object built in reducer
            // 3, CFG_OUTPUT_STATISTICS: cube statistic: hll of cuboids ...
            // 4, CFG_OUTPUT_PARTITION: dimension value range(min,max)
            JavaPairRDD<String, Tuple3<Writable, Writable, String>> outputRDD = aggredRDD
                    .mapPartitionsToPair(new MultiOutputFunction(cubeName, segmentId, metaUrl, sConf, samplingPercent));

            // make each reducer output to respective dir
            MultipleOutputs.addNamedOutput(job, BatchConstants.CFG_OUTPUT_COLUMN, SequenceFileOutputFormat.class,
                    NullWritable.class, Text.class);
            MultipleOutputs.addNamedOutput(job, BatchConstants.CFG_OUTPUT_DICT, SequenceFileOutputFormat.class,
                    NullWritable.class, ArrayPrimitiveWritable.class);
            MultipleOutputs.addNamedOutput(job, BatchConstants.CFG_OUTPUT_STATISTICS, SequenceFileOutputFormat.class,
                    LongWritable.class, BytesWritable.class);
            MultipleOutputs.addNamedOutput(job, BatchConstants.CFG_OUTPUT_PARTITION, TextOutputFormat.class,
                    NullWritable.class, LongWritable.class);

            FileOutputFormat.setOutputPath(job, new Path(outputPath));
            FileOutputFormat.setCompressOutput(job, false);

            // prevent to create zero-sized default output
            LazyOutputFormat.setOutputFormatClass(job, SequenceFileOutputFormat.class);

            MultipleOutputsRDD multipleOutputsRDD = MultipleOutputsRDD.rddToMultipleOutputsRDD(outputRDD);

            multipleOutputsRDD.saveAsNewAPIHadoopDatasetWithMultipleOutputs(job.getConfiguration());

            long recordCount = recordRDD.count();
            logger.info("Map input records={}", recordCount);
            logger.info("HDFS Read: {} HDFS Write", bytesWritten.value());

            Map<String, String> counterMap = Maps.newHashMap();
            counterMap.put(ExecutableConstants.SOURCE_RECORDS_COUNT, String.valueOf(recordCount));
            counterMap.put(ExecutableConstants.SOURCE_RECORDS_SIZE, String.valueOf(bytesWritten.value()));

            // save counter to hdfs
            HadoopUtil.writeToSequenceFile(sc.hadoopConfiguration(), counterPath, counterMap);

            HadoopUtil.deleteHDFSMeta(metaUrl);
            flatOutputRDD.unpersist(false);
        }
    }

    /**
     * output: Tuple2<SelfDefineSortableKey, Text>
     * 1, for statistics, SelfDefineSortableKey = reducerId + cuboidId, Text = hll of cuboidId
     * 2, for dict col, SelfDefineSortableKey = reducerId + field value, Text = ""
     * 3, for not dict col, SelfDefineSortableKey = reducerId + min/max value, Text = ""
     */
    static class FlatOutputFunction implements PairFlatMapFunction<Iterator<String[]>, SelfDefineSortableKey, Text> {
        private transient volatile boolean initialized = false;
        private String cubeName;
        private String segmentId;
        private String metaUrl;
        private SerializableConfiguration conf;
        private int samplingPercent;
        private LongAccumulator bytesWritten;

        private FactDistinctColumnsBase base;

        public FlatOutputFunction(String cubeName, String segmentId, String metaUrl, SerializableConfiguration conf,
                                  int samplingPercent, LongAccumulator bytesWritten) {
            this.cubeName = cubeName;
            this.segmentId = segmentId;
            this.metaUrl = metaUrl;
            this.conf = conf;
            this.samplingPercent = samplingPercent;
            this.bytesWritten = bytesWritten;
        }

        private void init() {
            base = new FactDistinctColumnsBase(cubeName, segmentId, metaUrl, conf, samplingPercent);
            base.setupMap();
            initialized = true;
        }

        @Override
        public Iterator<Tuple2<SelfDefineSortableKey, Text>> call(Iterator<String[]> rowIterator) throws Exception {
            if (initialized == false) {
                synchronized (SparkFactDistinct.class) {
                    if (initialized == false) {
                        init();
                    }
                }
            }

            List<Tuple2<SelfDefineSortableKey, Text>> result = Lists.newArrayList();

            FactDistinctColumnsBase.Visitor visitor = new FactDistinctColumnsBase.Visitor<SelfDefineSortableKey, Text>() {
                @Override
                public void collect(String namedOutput, SelfDefineSortableKey key, Text value, String outputPath) {
                    result.add(new Tuple2<>(key, value));
                }
            };

            while (rowIterator.hasNext()) {
                String[] row = rowIterator.next();
                bytesWritten.add(base.countSizeInBytes(row));
                base.map(row, visitor);
            }
            base.postMap(visitor);

            return result.iterator();
        }
    }

    static class FactDistinctPartitioner extends Partitioner {
        private transient volatile boolean initialized = false;
        private String cubeName;
        private String metaUrl;
        private SerializableConfiguration conf;
        private int totalReducerNum;
        private transient FactDistinctColumnsReducerMapping reducerMapping;

        public FactDistinctPartitioner(String cubeName, String metaUrl, SerializableConfiguration conf,
                                       int totalReducerNum) {
            this.cubeName = cubeName;
            this.metaUrl = metaUrl;
            this.conf = conf;
            this.totalReducerNum = totalReducerNum;
        }

        private void init() {
            KylinConfig kConfig = AbstractHadoopJob.loadKylinConfigFromHdfs(conf, metaUrl);
            try (KylinConfig.SetAndUnsetThreadLocalConfig autoUnset = KylinConfig
                    .setAndUnsetThreadLocalConfig(kConfig)) {
                CubeInstance cubeInstance = CubeManager.getInstance(kConfig).getCube(cubeName);
                reducerMapping = new FactDistinctColumnsReducerMapping(cubeInstance);
                initialized = true;
            }
        }

        @Override
        public int numPartitions() {
            return totalReducerNum;
        }

        @Override
        public int getPartition(Object o) {
            if (initialized == false) {
                synchronized (SparkFactDistinct.class) {
                    if (initialized == false) {
                        init();
                    }
                }
            }

            SelfDefineSortableKey skey = (SelfDefineSortableKey) o;
            Text key = skey.getText();
            if (key.getBytes()[0] == FactDistinctColumnsReducerMapping.MARK_FOR_HLL_COUNTER) {
                Long cuboidId = Bytes.toLong(key.getBytes(), 1, Bytes.SIZEOF_LONG);
                return reducerMapping.getReducerIdForCuboidRowCount(cuboidId);
            } else {
                return BytesUtil.readUnsigned(key.getBytes(), 0, 1);
            }
        }
    }

    static class MultiOutputFunction implements
            PairFlatMapFunction<Iterator<Tuple2<SelfDefineSortableKey, Text>>, String, Tuple3<Writable, Writable, String>> {
        private transient volatile boolean initialized = false;
        private String cubeName;
        private String segmentId;
        private String metaUrl;
        private SerializableConfiguration conf;
        private int samplingPercent;
        private int taskId;

        private FactDistinctColumnsBase base;

        public MultiOutputFunction(String cubeName, String segmentId, String metaurl, SerializableConfiguration conf,
                                   int samplingPercent) {
            this.cubeName = cubeName;
            this.segmentId = segmentId;
            this.metaUrl = metaurl;
            this.conf = conf;
            this.samplingPercent = samplingPercent;
        }

        private void init() throws IOException {
            taskId = TaskContext.getPartitionId();
            base = new FactDistinctColumnsBase(cubeName, segmentId, metaUrl, conf, samplingPercent);
            base.setupReduce(taskId);
            initialized = true;
        }

        @Override
        public Iterator<Tuple2<String, Tuple3<Writable, Writable, String>>> call(
                Iterator<Tuple2<SelfDefineSortableKey, Text>> tuple2Iterator) throws Exception {
            if (initialized == false) {
                synchronized (SparkFactDistinct.class) {
                    if (initialized == false) {
                        init();
                    }
                }
            }

            List<Tuple2<String, Tuple3<Writable, Writable, String>>> result = Lists.newArrayList();

            FactDistinctColumnsBase.Visitor visitor = new FactDistinctColumnsBase.Visitor<Writable, Writable>() {
                @Override
                public void collect(String namedOutput, Writable key, Writable value, String outputPath) {
                    result.add(new Tuple2<>(namedOutput, new Tuple3<>(key, value, outputPath)));
                }
            };

            while (tuple2Iterator.hasNext()) {
                Tuple2<SelfDefineSortableKey, Text> tuple = tuple2Iterator.next();
                base.reduce(new Pair<>(tuple._1, tuple._2), visitor);
            }
            base.postReduce(visitor);

            return result.iterator();
        }
    }
}
