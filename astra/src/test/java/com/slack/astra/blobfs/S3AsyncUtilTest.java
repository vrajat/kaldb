package com.slack.astra.blobfs;

import static org.assertj.core.api.Assertions.assertThat;

import java.net.URI;
import org.junit.jupiter.api.Test;

class S3AsyncUtilTest {
  @Test
  void shouldUseCompatibilityModeForLocalEndpoint() {
    assertThat(S3AsyncUtil.shouldUseS3CompatibleEndpointMode(URI.create("http://minio:9000")))
        .isTrue();
  }

  @Test
  void shouldUseCompatibilityModeForNonAwsHttpsEndpoint() {
    assertThat(
            S3AsyncUtil.shouldUseS3CompatibleEndpointMode(
                URI.create("https://objects.internal.example.com")))
        .isTrue();
  }

  @Test
  void shouldNotUseCompatibilityModeForAwsRegionalEndpoint() {
    assertThat(
            S3AsyncUtil.shouldUseS3CompatibleEndpointMode(
                URI.create("https://s3.us-east-1.amazonaws.com")))
        .isFalse();
  }

  @Test
  void shouldNotUseCompatibilityModeForAwsChinaEndpoint() {
    assertThat(
            S3AsyncUtil.shouldUseS3CompatibleEndpointMode(
                URI.create("https://s3.cn-north-1.amazonaws.com.cn")))
        .isFalse();
  }
}
