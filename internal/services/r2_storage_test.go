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
		albumCoverCache: make(map[string]AlbumCovers),
	}

	validWebP := append([]byte("RIFF\x20\x00\x00\x00WEBPVP8 "), []byte("sample-payload-bytes")...)
	url, err := svc.UploadCover(context.Background(), "hash123", validWebP, "image/webp")
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

func TestR2StorageService_RejectsNonImageAndOversized(t *testing.T) {
	svc := &R2StorageService{
		accountID:       "test-account",
		accessKeyID:     "test-key",
		secretAccessKey: "test-secret",
		bucketName:      "test-bucket",
		publicURL:       "https://cdn.example.com",
		httpClient: &http.Client{
			Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
				return &http.Response{
					StatusCode: http.StatusOK,
					Body:       io.NopCloser(strings.NewReader("")),
				}, nil
			}),
		},
	}

	ctx := context.Background()

	// 1. Rejects MP3 audio starting with ID3
	mp3Payload := []byte("ID3\x04\x00\x00\x00\x00\x00audio-tag-bytes")
	if _, err := svc.UploadCover(ctx, "hash_mp3", mp3Payload, "image/jpeg"); err == nil {
		t.Fatal("Expected error when uploading MP3 audio bytes, got nil")
	}

	// 2. Rejects M4A audio starting with ftyp
	m4aPayload := []byte("\x00\x00\x00\x1cftypM4A \x00\x00\x00\x00")
	if _, err := svc.UploadCover(ctx, "hash_m4a", m4aPayload, "image/jpeg"); err == nil {
		t.Fatal("Expected error when uploading M4A audio bytes, got nil")
	}

	// 3. Rejects FLAC audio starting with fLaC
	flacPayload := []byte("fLaC\x00\x00\x00\x22audio-data")
	if _, err := svc.UploadCover(ctx, "hash_flac", flacPayload, "image/jpeg"); err == nil {
		t.Fatal("Expected error when uploading FLAC audio bytes, got nil")
	}

	// 4. Rejects oversized payload > 5MB
	oversized := make([]byte, MaxCoverPayloadBytes+1)
	oversized[0], oversized[1], oversized[2] = 0xFF, 0xD8, 0xFF // valid JPEG header but oversized
	if _, err := svc.UploadCover(ctx, "hash_big", oversized, "image/jpeg"); err == nil {
		t.Fatal("Expected error when uploading oversized payload, got nil")
	}

	// 5. Accepts genuine JPEG
	validJPEG := []byte{0xFF, 0xD8, 0xFF, 0xE0, 0x00, 0x10, 'J', 'F', 'I', 'F', 0x00}
	url, err := svc.UploadCover(ctx, "valid_jpeg", validJPEG, "")
	if err != nil {
		t.Fatalf("Expected valid JPEG to succeed, got %v", err)
	}
	if !strings.HasSuffix(url, ".jpg") {
		t.Fatalf("Expected .jpg extension for JPEG, got %s", url)
	}
}

func TestR2StorageService_DeleteCoverMock(t *testing.T) {
	var receivedMethod string
	var receivedKey string

	mockTransport := roundTripFunc(func(req *http.Request) (*http.Response, error) {
		receivedMethod = req.Method
		receivedKey = req.URL.Path
		return &http.Response{
			StatusCode: http.StatusNoContent,
			Body:       io.NopCloser(strings.NewReader("")),
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
	}

	err := svc.DeleteCover(context.Background(), "covers/bad_audio.jpg")
	if err != nil {
		t.Fatalf("DeleteCover failed: %v", err)
	}
	if receivedMethod != http.MethodDelete {
		t.Fatalf("Expected HTTP DELETE, got %s", receivedMethod)
	}
	if receivedKey != "/test-bucket/covers/bad_audio.jpg" {
		t.Fatalf("Unexpected S3 key path: %s", receivedKey)
	}
}

type roundTripFunc func(req *http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}
