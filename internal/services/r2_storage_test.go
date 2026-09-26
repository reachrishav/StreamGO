package services

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"streamgo/internal/config"
)

func TestR2StorageService_Configured(t *testing.T) {
	// 1. Unconfigured
	unconfigured := NewR2StorageService(&config.Config{})
	if unconfigured.IsConfigured() {
		t.Fatal("Expected IsConfigured to be false for empty config")
	}

	// 2. Configured
	configured := NewR2StorageService(&config.Config{
		R2AccountID:       "acc123",
		R2AccessKeyID:     "key123",
		R2SecretAccessKey: "sec123",
		R2BucketName:      "covers-bucket",
		R2PublicURL:       "https://covers.cdn.streamx.live",
	})
	if !configured.IsConfigured() {
		t.Fatal("Expected IsConfigured to be true for complete config")
	}

	// 3. Album cache
	if hit := configured.GetAlbumCover("alb_1"); hit != "" {
		t.Fatalf("Expected empty hit for uncached album, got %s", hit)
	}
	configured.SetAlbumCover("alb_1", "https://covers.cdn.streamx.live/covers/abc.webp")
	if hit := configured.GetAlbumCover("alb_1"); hit != "https://covers.cdn.streamx.live/covers/abc.webp" {
		t.Fatalf("Expected cached URL, got %s", hit)
	}
}

func TestR2StorageService_SigV4Signing(t *testing.T) {
	req, err := http.NewRequest(http.MethodPut, "https://acc123.r2.cloudflarestorage.com/covers-bucket/covers/test.webp", nil)
	if err != nil {
		t.Fatalf("Failed to create request: %v", err)
	}

	req.Header.Set("Host", "acc123.r2.cloudflarestorage.com")
	req.Header.Set("Content-Type", "image/webp")
	req.Header.Set("Cache-Control", "public, max-age=31536000, immutable")

	payload := []byte("dummy-webp-data")
	fixedTime := time.Date(2026, 9, 26, 12, 0, 0, 0, time.UTC)

	signS3Request(req, payload, "MY_KEY", "MY_SECRET", "auto", "s3", fixedTime)

	auth := req.Header.Get("Authorization")
	if !strings.HasPrefix(auth, "AWS4-HMAC-SHA256 Credential=MY_KEY/20260926/auto/s3/aws4_request") {
		t.Fatalf("Unexpected Authorization header prefix: %s", auth)
	}
	if !strings.Contains(auth, "SignedHeaders=cache-control;content-type;host;x-amz-content-sha256;x-amz-date") {
		t.Fatalf("Authorization header missing signed headers: %s", auth)
	}
	if !strings.Contains(auth, "Signature=") {
		t.Fatalf("Authorization header missing signature: %s", auth)
	}
}

func TestR2StorageService_UploadMock(t *testing.T) {
	var receivedAuth string
	var receivedKey string
	var receivedContentType string

	mockTransport := roundTripFunc(func(req *http.Request) (*http.Response, error) {
		receivedAuth = req.Header.Get("Authorization")
		receivedKey = req.URL.Path
		receivedContentType = req.Header.Get("Content-Type")

		return &http.Response{
			StatusCode: http.StatusOK,
			Body:       io.NopCloser(strings.NewReader("")),
			Header:     make(http.Header),
		}, nil
	})

	svc := &R2StorageService{
		accountID:       "test-account",
		accessKeyID:     "test-key",
		secretAccessKey: "test-secret",
		bucketName:      "test-bucket",
		publicURL:       "https://cdn.example.com",
		httpClient: &http.Client{
			Transport: mockTransport,
		},
		albumCoverCache: make(map[string]string),
	}

	url, err := svc.UploadCover(context.Background(), "hash123", []byte("sample-data"), "image/webp")
	if err != nil {
		t.Fatalf("UploadCover failed: %v", err)
	}

	if url != "https://cdn.example.com/covers/hash123.webp" {
		t.Fatalf("Unexpected public URL: %s", url)
	}
	if !strings.Contains(receivedAuth, "AWS4-HMAC-SHA256") {
		t.Fatalf("Server did not receive AWS SigV4 authorization header: %s", receivedAuth)
	}
	if receivedKey != "/test-bucket/covers/hash123.webp" {
		t.Fatalf("Unexpected S3 key path: %s", receivedKey)
	}
	if receivedContentType != "image/webp" {
		t.Fatalf("Unexpected Content-Type: %s", receivedContentType)
	}
}

type roundTripFunc func(req *http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}
