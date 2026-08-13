package com.slack.astra.blobfs.nrt;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.slack.astra.blobfs.BlobStore;
import com.slack.astra.blobfs.S3AsyncUtil;
import com.slack.astra.chunk.ReadWriteChunk;
import com.slack.astra.logstore.LogStore;
import com.slack.astra.metadata.schema.LuceneFieldDef;
import com.slack.astra.metadata.snapshot.SnapshotMetadata;
import com.slack.astra.metadata.snapshot.SnapshotMetadataStore;
import com.slack.astra.proto.config.AstraConfigs;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.store.FSDirectory;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;

class NrtSnapshotPublisherTest {
  private static final String SNAPSHOT_ID = "LIVE_chunk-1";
  private static final String PARTITION_ID = "0";

  @Test
  void testSuccessfulPublishUpdatesManifestAndSnapshotMetadata() throws Exception {
    TestContext context = testContext("", 0);

    SnapshotMetadata updated =
        context.publisher.publish(context.logStore, context.snapshotMetadata, 10);

    assertThat(updated.snapshotGeneration).isEqualTo(1);
    assertThat(updated.snapshotPath).isEqualTo(context.snapshotPath);
    verify(context.snapshotMetadataStore).updateSync(updated);

    ArgumentCaptor<String> manifestCaptor = ArgumentCaptor.forClass(String.class);
    verify(context.blobStore)
        .uploadData(eq(context.snapshotPath), manifestCaptor.capture(), eq(false));
    NrtBlobStore.NrtManifest manifest = NrtBlobStore.deserializeManifest(manifestCaptor.getValue());
    assertThat(manifest.manifestGeneration()).isEqualTo(1);
    assertThat(manifest.startOffsetInclusive()).isEqualTo(10);
    assertThat(manifest.schemaFile().key())
        .isEqualTo(context.filesPath + "/" + ReadWriteChunk.SCHEMA_FILE_NAME);
    assertThat(manifest.schemaFile().checksum()).hasSize(64);
    assertThat(manifest.files())
        .extracting(NrtBlobStore.FileEntry::name)
        .containsExactly("segments_1");
    assertThat(manifest.files())
        .extracting(NrtBlobStore.FileEntry::checksum)
        .allMatch(c -> c.length() == 64);
  }

  @Test
  void testFileUploadFailureDoesNotWriteManifestOrMetadata() throws Exception {
    TestContext context = testContext("", 0);
    doThrow(new RuntimeException("upload failed"))
        .when(context.blobStore)
        .upload(anyString(), any());

    assertThatThrownBy(
            () -> context.publisher.publish(context.logStore, context.snapshotMetadata, 0))
        .isInstanceOf(RuntimeException.class)
        .hasMessageContaining("upload failed");

    verify(context.blobStore, never()).uploadData(anyString(), anyString(), anyBoolean());
    verify(context.snapshotMetadataStore, never()).updateSync(any());
  }

  @Test
  void testManifestUploadFailureDoesNotUpdateMetadata() throws Exception {
    TestContext context = testContext("", 0);
    doThrow(new RuntimeException("manifest failed"))
        .when(context.blobStore)
        .uploadData(anyString(), anyString(), anyBoolean());

    assertThatThrownBy(
            () -> context.publisher.publish(context.logStore, context.snapshotMetadata, 0))
        .isInstanceOf(RuntimeException.class)
        .hasMessageContaining("manifest failed");

    verify(context.snapshotMetadataStore, never()).updateSync(any());
  }

  @Test
  void testMetadataUpdateFailureLeavesManifestWritten() throws Exception {
    TestContext context = testContext("", 0);
    doThrow(new RuntimeException("metadata failed"))
        .when(context.snapshotMetadataStore)
        .updateSync(any());

    assertThatThrownBy(
            () -> context.publisher.publish(context.logStore, context.snapshotMetadata, 0))
        .isInstanceOf(RuntimeException.class)
        .hasMessageContaining("metadata failed");

    verify(context.blobStore).uploadData(eq(context.snapshotPath), anyString(), eq(false));
  }

  @Test
  void testExistingManifestDoesNotBlockSingleWriterPublish() throws Exception {
    TestContext context = testContext("previous-manifest.json", 1);
    NrtBlobStore.NrtManifest currentManifest = manifest(context.filesPath, 1);
    when(context.blobStore.readFileData("previous-manifest.json", false))
        .thenReturn(NrtBlobStore.serializeManifest(currentManifest));

    SnapshotMetadata updated =
        context.publisher.publish(context.logStore, context.snapshotMetadata, 0);

    assertThat(updated.snapshotGeneration).isEqualTo(2);
    verify(context.blobStore).upload(anyString(), any());
    verify(context.blobStore).uploadData(eq(context.snapshotPath), anyString(), eq(false));
    verify(context.snapshotMetadataStore).updateSync(updated);
  }

  @Test
  void testConfiguredS3SmokePublishRoundTrip() throws Exception {
    AstraConfigs.S3Config s3Config = s3SmokeConfig();
    String bucket = s3Config.getS3Bucket();
    assumeTrue(bucket != null && !bucket.isBlank(), "NRT_S3_SMOKE_BUCKET must be set");

    try (var s3Client = S3AsyncUtil.initS3Client(s3Config)) {
      BlobStore blobStore = new BlobStore(s3Client, bucket, s3Config.getS3PathPrefix());
      try {
        TestContext context = realS3TestContext(blobStore);
        SnapshotMetadata updated =
            context.publisher.publish(context.logStore, context.snapshotMetadata, 10);
        NrtBlobStore.NrtManifest manifest = context.nrtBlobStore.readManifest(updated.snapshotPath);

        assertThat(updated.snapshotGeneration).isEqualTo(1);
        assertThat(updated.snapshotPath)
            .startsWith(
                "nrt/v1/partitions/" + PARTITION_ID + "/chunks/" + SNAPSHOT_ID + "/manifests/");
        assertThat(manifest.snapshotId()).isEqualTo(SNAPSHOT_ID);
        assertThat(manifest.manifestGeneration()).isEqualTo(1);
        assertThat(manifest.startOffsetInclusive()).isEqualTo(10);
        assertThat(manifest.schemaFile().key())
            .isEqualTo(context.filesPath + "/" + ReadWriteChunk.SCHEMA_FILE_NAME);

        String prefixedManifestKey =
            s3Config.getS3PathPrefix().isBlank()
                ? updated.snapshotPath
                : s3Config.getS3PathPrefix() + "/" + updated.snapshotPath;
        s3Client
            .headObject(HeadObjectRequest.builder().bucket(bucket).key(prefixedManifestKey).build())
            .get();
      } finally {
        if (!Boolean.parseBoolean(
            System.getenv().getOrDefault("NRT_S3_SMOKE_SKIP_CLEANUP", "false"))) {
          blobStore.delete("nrt");
        }
      }
    }
  }

  private static TestContext testContext(String currentSnapshotPath, long generation)
      throws Exception {
    Path indexDirectory = Files.createTempDirectory("nrt-publisher-test");
    Files.writeString(indexDirectory.resolve("segments_1"), "segment data");

    BlobStore blobStore = mock(BlobStore.class);
    NrtBlobStore nrtBlobStore = new NrtBlobStore(blobStore);
    SnapshotMetadataStore snapshotMetadataStore = mock(SnapshotMetadataStore.class);
    LogStore logStore = mock(LogStore.class);
    IndexCommit indexCommit = mock(IndexCommit.class);
    FSDirectory directory = FSDirectory.open(indexDirectory);
    ConcurrentHashMap<String, LuceneFieldDef> schema = new ConcurrentHashMap<>();
    schema.put("message", new LuceneFieldDef("message", "keyword", true, true, false));

    when(logStore.getDirectory()).thenReturn(directory);
    when(logStore.getIndexCommit()).thenReturn(indexCommit);
    when(logStore.getSchema()).thenReturn(schema);
    when(indexCommit.getFileNames()).thenReturn(List.of("segments_1"));
    when(indexCommit.getGeneration()).thenReturn(7L);

    String snapshotPath =
        NrtBlobStore.manifestPath(PARTITION_ID, SNAPSHOT_ID, generation + 1, "indexer-1");
    String filesPath = NrtBlobStore.filesPath(PARTITION_ID, SNAPSHOT_ID, generation + 1);
    SnapshotMetadata snapshotMetadata =
        new SnapshotMetadata(
            SNAPSHOT_ID,
            1000,
            2000,
            25,
            PARTITION_ID,
            0,
            SnapshotMetadata.SnapshotType.LIVE,
            SnapshotMetadata.IndexType.LUCENE,
            currentSnapshotPath,
            generation,
            SnapshotMetadata.DEFAULT_VERSION);

    return new TestContext(
        blobStore,
        nrtBlobStore,
        snapshotMetadataStore,
        logStore,
        new NrtSnapshotPublisher(blobStore, nrtBlobStore, snapshotMetadataStore, "indexer-1"),
        snapshotMetadata,
        snapshotPath,
        filesPath);
  }

  private static TestContext realS3TestContext(BlobStore blobStore) throws Exception {
    Path indexDirectory = Files.createTempDirectory("nrt-publisher-s3-smoke");
    Files.writeString(indexDirectory.resolve("segments_1"), "segment data");

    NrtBlobStore nrtBlobStore = new NrtBlobStore(blobStore);
    SnapshotMetadataStore snapshotMetadataStore = mock(SnapshotMetadataStore.class);
    LogStore logStore = mock(LogStore.class);
    IndexCommit indexCommit = mock(IndexCommit.class);
    FSDirectory directory = FSDirectory.open(indexDirectory);
    ConcurrentHashMap<String, LuceneFieldDef> schema = new ConcurrentHashMap<>();
    schema.put("message", new LuceneFieldDef("message", "keyword", true, true, false));

    when(logStore.getDirectory()).thenReturn(directory);
    when(logStore.getIndexCommit()).thenReturn(indexCommit);
    when(logStore.getSchema()).thenReturn(schema);
    when(indexCommit.getFileNames()).thenReturn(List.of("segments_1"));
    when(indexCommit.getGeneration()).thenReturn(7L);

    String snapshotPath = NrtBlobStore.manifestPath(PARTITION_ID, SNAPSHOT_ID, 1, "indexer-1");
    String filesPath = NrtBlobStore.filesPath(PARTITION_ID, SNAPSHOT_ID, 1);
    SnapshotMetadata snapshotMetadata =
        new SnapshotMetadata(
            SNAPSHOT_ID,
            1000,
            2000,
            25,
            PARTITION_ID,
            0,
            SnapshotMetadata.SnapshotType.LIVE,
            SnapshotMetadata.IndexType.LUCENE,
            "",
            0,
            SnapshotMetadata.DEFAULT_VERSION);

    return new TestContext(
        blobStore,
        nrtBlobStore,
        snapshotMetadataStore,
        logStore,
        new NrtSnapshotPublisher(blobStore, nrtBlobStore, snapshotMetadataStore, "indexer-1"),
        snapshotMetadata,
        snapshotPath,
        filesPath);
  }

  private static AstraConfigs.S3Config s3SmokeConfig() {
    String bucket = System.getenv("NRT_S3_SMOKE_BUCKET");
    String region = System.getenv().getOrDefault("NRT_S3_SMOKE_REGION", "us-east-1");
    String endpoint = System.getenv().getOrDefault("NRT_S3_SMOKE_ENDPOINT", "");
    String accessKey = System.getenv().getOrDefault("NRT_S3_SMOKE_ACCESS_KEY", "");
    String secretKey = System.getenv().getOrDefault("NRT_S3_SMOKE_SECRET_KEY", "");
    String pathPrefix =
        System.getenv()
            .getOrDefault("NRT_S3_SMOKE_PREFIX", "nrt-smoke/" + System.currentTimeMillis());

    AstraConfigs.S3Config.Builder builder =
        AstraConfigs.S3Config.newBuilder()
            .setS3Bucket(bucket == null ? "" : bucket)
            .setS3Region(region)
            .setS3PathPrefix(pathPrefix)
            .setS3TargetThroughputGbps(10D);
    if (!endpoint.isBlank()) {
      builder.setS3EndPoint(endpoint);
    }
    if (!accessKey.isBlank() && !secretKey.isBlank()) {
      builder.setS3AccessKey(accessKey).setS3SecretKey(secretKey);
    }
    return builder.build();
  }

  private static NrtBlobStore.NrtManifest manifest(String filesPath, long manifestGeneration) {
    return new NrtBlobStore.NrtManifest(
        1,
        SNAPSHOT_ID,
        PARTITION_ID,
        "indexer-1",
        manifestGeneration,
        1000,
        7,
        0,
        25,
        1000,
        2000,
        new NrtBlobStore.FileEntry(
            ReadWriteChunk.SCHEMA_FILE_NAME,
            filesPath + "/" + ReadWriteChunk.SCHEMA_FILE_NAME,
            10,
            "10"),
        List.of(new NrtBlobStore.FileEntry("segments_1", filesPath + "/segments_1", 12, "12")));
  }

  private record TestContext(
      BlobStore blobStore,
      NrtBlobStore nrtBlobStore,
      SnapshotMetadataStore snapshotMetadataStore,
      LogStore logStore,
      NrtSnapshotPublisher publisher,
      SnapshotMetadata snapshotMetadata,
      String snapshotPath,
      String filesPath) {}
}
