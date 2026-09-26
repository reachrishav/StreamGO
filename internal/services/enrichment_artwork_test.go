package services

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"

	"streamgo/internal/config"
)

func TestEnrichment_ArtworkR2Optional(t *testing.T) {
	// 1. Test Unconfigured R2 (Free Mode)
	cfgFree := &config.Config{}
	r2Free := NewR2StorageService(cfgFree)
	if r2Free.IsConfigured() {
		t.Fatal("Expected R2 to be unconfigured for free mode")
	}

	enrichSvc := NewEnrichmentService(nil, NewCoverSearchService(), nil)
	enrichSvc.SetR2Storage(r2Free)

	if enrichSvc.r2Storage.IsConfigured() {
		t.Fatal("EnrichmentService should report R2 as not configured")
	}

	// 2. Test Configured R2 Mode
	cfgR2 := &config.Config{
		R2AccountID:       "acc_test",
		R2AccessKeyID:     "key_test",
		R2SecretAccessKey: "secret_test",
		R2BucketName:      "covers-test",
		R2PublicURL:       "https://covers.cdn.streamx.live",
	}
	r2Configured := NewR2StorageService(cfgR2)
	if !r2Configured.IsConfigured() {
		t.Fatal("Expected R2 to be configured")
	}

	enrichSvc.SetR2Storage(r2Configured)
	if !enrichSvc.r2Storage.IsConfigured() {
		t.Fatal("EnrichmentService should report R2 as configured")
	}

	// Test Sibling Cache on EnrichmentService's R2Storage
	albumID := "album_test_123"
	if hit := enrichSvc.r2Storage.GetAlbumCover(albumID); hit != "" {
		t.Fatalf("Expected empty hit for album, got %s", hit)
	}

	mockCoverURL := "https://covers.cdn.streamx.live/covers/abc12345.webp"
	enrichSvc.r2Storage.SetAlbumCover(albumID, mockCoverURL)

	if hit := enrichSvc.r2Storage.GetAlbumCover(albumID); hit != mockCoverURL {
		t.Fatalf("Expected cached R2 cover URL %s, got %s", mockCoverURL, hit)
	}
}

func TestEnrichment_ExtractCompressAndUpload(t *testing.T) {
	// Generate mock MP4 chunk with JPEG
	testJPEG := createTestJPEG(200, 200)
	chunk := createTestMP4CovrChunk(testJPEG)

	artworkSvc := NewArtworkService()

	// 1. Extract
	extracted, mime, err := artworkSvc.ExtractArtwork(chunk, "alac")
	if err != nil {
		t.Fatalf("Failed to extract artwork: %v", err)
	}
	if mime != "image/jpeg" {
		t.Fatalf("Expected image/jpeg, got %s", mime)
	}

	// 2. Compress to WebP
	webpBytes, err := artworkSvc.CompressToWebP(context.Background(), extracted, 150, 80)
	if err != nil {
		t.Fatalf("CompressToWebP failed: %v", err)
	}
	if len(webpBytes) == 0 {
		t.Fatal("Expected non-empty compressed WebP")
	}

	// 3. Upload to Mock R2
	var receivedPath string
	mockClient := roundTripFunc(func(req *http.Request) (*http.Response, error) {
		receivedPath = req.URL.Path
		return &http.Response{
			StatusCode: http.StatusOK,
			Body:       io.NopCloser(strings.NewReader("")),
			Header:     make(http.Header),
		}, nil
	})

	r2Svc := &R2StorageService{
		accountID:       "acc_test",
		accessKeyID:     "key_test",
		secretAccessKey: "secret_test",
		bucketName:      "covers-test",
		publicURL:       "https://covers.cdn.streamx.live",
		httpClient: &http.Client{
			Transport: mockClient,
		},
		albumCoverCache: make(map[string]string),
	}

	hash := sha256Hex(webpBytes)
	url, err := r2Svc.UploadCover(context.Background(), hash, webpBytes, "image/webp")
	if err != nil {
		t.Fatalf("UploadCover failed: %v", err)
	}

	expectedPrefix := "https://covers.cdn.streamx.live/covers/"
	if !strings.HasPrefix(url, expectedPrefix) || !strings.HasSuffix(url, ".webp") {
		t.Fatalf("Unexpected R2 cover URL: %s", url)
	}
	if !strings.HasPrefix(receivedPath, "/covers-test/covers/") {
		t.Fatalf("Unexpected request path: %s", receivedPath)
	}
}
