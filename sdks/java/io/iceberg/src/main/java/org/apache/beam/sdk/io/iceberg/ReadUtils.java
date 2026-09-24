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
package org.apache.beam.sdk.io.iceberg;

import static org.apache.iceberg.util.SnapshotUtil.ancestorsOf;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.beam.sdk.io.iceberg.IcebergIO.ReadRows.StartingStrategy;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.MoreObjects;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.ContentScanTask;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.data.InternalRecordWrapper;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.encryption.EncryptedFiles;
import org.apache.iceberg.encryption.EncryptedInputFile;
import org.apache.iceberg.expressions.Evaluator;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.formats.FormatModelRegistry;
import org.apache.iceberg.formats.ReadBuilder;
import org.apache.iceberg.hadoop.HadoopInputFile;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.mapping.NameMapping;
import org.apache.iceberg.mapping.NameMappingParser;
import org.apache.iceberg.util.SnapshotUtil;
import org.checkerframework.checker.nullness.qual.Nullable;

/** Helper class for source operations. */
public class ReadUtils {
  // default is 8MB. keep this low to avoid overwhelming memory
  static final int MAX_FILE_BUFFER_SIZE = 1 << 18; // 256KB

  public static CloseableIterable<Record> createReader(
      ContentScanTask<?> task, Table table, IcebergScanConfig scanConfig) {
    return createReader(
        table,
        scanConfig,
        scanConfig.getRequiredSchema(),
        task.spec(),
        task.file(),
        null,
        task.start(),
        task.length(),
        task.residual());
  }

  public static CloseableIterable<Record> createReader(
      Table table,
      IcebergScanConfig scanConfig,
      Schema requiredSchema,
      PartitionSpec spec,
      ContentFile<?> file,
      @Nullable Long fileSequenceNumber,
      long start,
      long length,
      Expression residual) {
    InputFile rawInputFile =
        configureParquetReadInput(file.format(), table.io().newInputFile(file.location()));
    EncryptedInputFile encryptedInput =
        EncryptedFiles.encryptedInput(rawInputFile, file.keyMetadata());
    InputFile inputFile = table.encryption().decrypt(encryptedInput);
    Map<Integer, ?> idToConstants = PartitionUtils.constantsMap(spec, file, fileSequenceNumber);

    @Nullable String nameMapping = table.properties().get(TableProperties.DEFAULT_NAME_MAPPING);
    NameMapping mapping =
        nameMapping != null ? NameMappingParser.fromJson(nameMapping) : NameMapping.empty();

    ReadBuilder<Record, ?> readerBuilder =
        FormatModelRegistry.readBuilder(file.format(), Record.class, inputFile)
            .split(start, length)
            .project(requiredSchema)
            .filter(residual)
            .idToConstant(idToConstants)
            .withNameMapping(mapping);
    CloseableIterable<Record> records = readerBuilder.build();
    return maybeApplyFilter(records, scanConfig, requiredSchema);
  }

  static InputFile configureParquetReadInput(FileFormat fileFormat, InputFile inputFile) {
    // This configures normal (non-vectored) chunk allocation through HadoopInputFile only.
    // Vectored reads bypass the allocation cap, and non-Hadoop inputs have no equivalent hook
    // in Iceberg's current generic reader builder.
    if (fileFormat != FileFormat.PARQUET || !(inputFile instanceof HadoopInputFile)) {
      return inputFile;
    }

    HadoopInputFile hadoopInputFile = (HadoopInputFile) inputFile;
    Configuration conf = new Configuration(hadoopInputFile.getConf());
    conf.setInt("parquet.read.allocation.size", MAX_FILE_BUFFER_SIZE);
    return HadoopInputFile.fromPath(
        hadoopInputFile.getPath(), hadoopInputFile.getFileSystem(), conf);
  }

  public static @Nullable Long getFromSnapshotInclusive(Table table, IcebergScanConfig scanConfig) {
    @Nullable StartingStrategy startingStrategy = scanConfig.getStartingStrategy();
    boolean isStreaming = MoreObjects.firstNonNull(scanConfig.getStreaming(), false);
    if (startingStrategy == null) {
      startingStrategy = isStreaming ? StartingStrategy.LATEST : StartingStrategy.EARLIEST;
    }

    // 1. fetch from from_snapshot
    @Nullable Long fromSnapshot = scanConfig.getFromSnapshotInclusive();
    // 2. fetch from from_timestamp
    @Nullable Long fromTimestamp = scanConfig.getFromTimestamp();
    if (fromTimestamp != null) {
      fromSnapshot = SnapshotUtil.oldestAncestorAfter(table, fromTimestamp).snapshotId();
    }
    // 3. get current snapshot if starting_strategy is LATEST
    if (fromSnapshot == null && startingStrategy.equals(StartingStrategy.LATEST)) {
      @Nullable Snapshot currentSnapshot = table.currentSnapshot();
      if (currentSnapshot != null) {
        fromSnapshot = currentSnapshot.snapshotId();
      }
    }

    return fromSnapshot;
  }

  public static @Nullable Long getFromSnapshotExclusive(Table table, IcebergScanConfig scanConfig) {
    @Nullable Long fromSnapshot = getFromSnapshotInclusive(table, scanConfig);

    // incremental append scan can only be configured with an *exclusive* starting snapshot,
    // so we need to provide this snapshot's parent id.
    if (fromSnapshot != null) {
      Snapshot snapshot = table.snapshot(fromSnapshot);
      fromSnapshot = snapshot != null ? snapshot.parentId() : null;
    }

    // if snapshot is still null, the scan will default to the oldest snapshot, i.e. EARLIEST
    return fromSnapshot;
  }

  public static @Nullable Long getToSnapshot(Table table, IcebergScanConfig scanConfig) {
    // 1. fetch from to_snapshot
    @Nullable Long toSnapshot = scanConfig.getToSnapshot();
    // 2. fetch from to_timestamp
    @Nullable Long toTimestamp = scanConfig.getToTimestamp();
    if (toTimestamp != null) {
      toSnapshot = SnapshotUtil.snapshotIdAsOfTime(table, toTimestamp);
    }

    return toSnapshot;
  }

  /**
   * Returns a list of snapshots in the range (fromSnapshotId, toSnapshotId], ordered
   * chronologically.
   */
  public static List<SnapshotInfo> snapshotsBetween(
      Table table, String tableIdentifier, @Nullable Long fromSnapshotId, long toSnapshotId) {
    long from = MoreObjects.firstNonNull(fromSnapshotId, -1L);
    @SuppressWarnings("return")
    List<SnapshotInfo> snapshotIds =
        Lists.newArrayList(
                Lists.newArrayList(
                    ancestorsOf(
                        toSnapshotId,
                        snapshotId -> snapshotId != from ? table.snapshot(snapshotId) : null)))
            .stream()
            .map(s -> SnapshotInfo.fromSnapshot(s, tableIdentifier))
            .sorted(Comparator.comparingLong(SnapshotInfo::getSequenceNumber))
            .collect(Collectors.toList());

    return snapshotIds;
  }

  public static CloseableIterable<Record> maybeApplyFilter(
      CloseableIterable<Record> iterable, IcebergScanConfig scanConfig) {
    return maybeApplyFilter(iterable, scanConfig, scanConfig.getRequiredSchema());
  }

  public static CloseableIterable<Record> maybeApplyFilter(
      CloseableIterable<Record> iterable, IcebergScanConfig scanConfig, Schema requiredSchema) {
    InternalRecordWrapper wrapper = new InternalRecordWrapper(requiredSchema.asStruct());
    Expression filter = scanConfig.getFilter();
    Evaluator evaluator = scanConfig.getEvaluator(requiredSchema);
    if (filter != null && evaluator != null && filter.op() != Expression.Operation.TRUE) {
      return CloseableIterable.filter(iterable, record -> evaluator.eval(wrapper.wrap(record)));
    }
    return iterable;
  }
}
