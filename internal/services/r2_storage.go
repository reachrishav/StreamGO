package services

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"streamgo/internal/config"
	"streamgo/internal/logger"
)

var logR2 = logger.New("r2_storage")

// R2StorageService provides optional Cloudflare R2 / S3-compatible blob storage
// for persistent, CDN-delivered artwork thumbnails.
type R2StorageService struct {
	accountID       string
	accessKeyID     string
	secretAccessKey string
	bucketName      string
	publicURL       string
	httpClient      *http.Client

	mu              sync.RWMutex
	albumCoverCache map[string]string // albumID -> publicCoverURL
}

// NewR2StorageService creates a new R2StorageService from application config.
func NewR2StorageService(cfg *config.Config) *R2StorageService {
	if cfg == nil || !cfg.HasR2() {
		return &R2StorageService{
			albumCoverCache: make(map[string]string),
		}
	}

	publicURL := strings.TrimRight(cfg.R2PublicURL, "/")
	if publicURL == "" {
		// Fallback to standard R2 dev or custom endpoint format
		publicURL = fmt.Sprintf("https://%s.r2.cloudflarestorage.com/%s", cfg.R2AccountID, cfg.R2BucketName)
	}

	return &R2StorageService{
		accountID:       cfg.R2AccountID,
		accessKeyID:     cfg.R2AccessKeyID,
		secretAccessKey: cfg.R2SecretAccessKey,
		bucketName:      cfg.R2BucketName,
		publicURL:       publicURL,
		httpClient: &http.Client{
			Timeout: 15 * time.Second,
		},
		albumCoverCache: make(map[string]string),
	}
}

// IsConfigured returns true if all required R2 credentials are present.
func (s *R2StorageService) IsConfigured() bool {
	return s != nil && s.accountID != "" && s.accessKeyID != "" && s.secretAccessKey != "" && s.bucketName != ""
}

// GetAlbumCover returns the cached public R2 cover URL for an album, if present.
func (s *R2StorageService) GetAlbumCover(albumID string) string {
	if albumID == "" {
		return ""
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.albumCoverCache[albumID]
}

// SetAlbumCover caches the public R2 cover URL for an album.
func (s *R2StorageService) SetAlbumCover(albumID, coverURL string) {
	if albumID == "" || coverURL == "" {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.albumCoverCache[albumID] = coverURL
}

// UploadCover uploads artwork bytes to Cloudflare R2 using standard S3 SigV4 authentication.
// Returns the public CDN URL for the uploaded object.
func (s *R2StorageService) UploadCover(ctx context.Context, hash string, data []byte, contentType string) (string, error) {
	if !s.IsConfigured() {
		return "", fmt.Errorf("r2 storage is not configured")
	}
	if len(data) == 0 {
		return "", fmt.Errorf("cannot upload empty data")
	}

	ext := "webp"
	if contentType == "image/jpeg" {
		ext = "jpg"
	}
	key := fmt.Sprintf("covers/%s.%s", hash, ext)

	endpointHost := fmt.Sprintf("%s.r2.cloudflarestorage.com", s.accountID)
	reqURL := fmt.Sprintf("https://%s/%s/%s", endpointHost, s.bucketName, key)

	req, err := http.NewRequestWithContext(ctx, http.MethodPut, reqURL, bytes.NewReader(data))
	if err != nil {
		return "", fmt.Errorf("failed to create upload request: %w", err)
	}

	if contentType == "" {
		contentType = "image/webp"
	}
	cacheControl := "public, max-age=31536000, immutable"

	req.Header.Set("Content-Type", contentType)
	req.Header.Set("Cache-Control", cacheControl)
	req.Header.Set("Host", endpointHost)

	// Sign request with AWS SigV4
	now := time.Now().UTC()
	signS3Request(req, data, s.accessKeyID, s.secretAccessKey, "auto", "s3", now)

	resp, err := s.httpClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("r2 upload request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusNoContent {
		return "", fmt.Errorf("r2 upload returned status %d", resp.StatusCode)
	}

	publicURL := fmt.Sprintf("%s/%s", s.publicURL, key)
	logR2.Infof("Successfully uploaded artwork to R2: %s (%.1f KB)", key, float64(len(data))/1024.0)
	return publicURL, nil
}

// signS3Request signs an HTTP request using AWS Signature Version 4 (SigV4).
func signS3Request(req *http.Request, payload []byte, accessKey, secretKey, region, service string, t time.Time) {
	dateStamp := t.Format("20060102")
	amzDate := t.Format("20060102T150405Z")

	payloadHashBytes := sha256.Sum256(payload)
	payloadHash := hex.EncodeToString(payloadHashBytes[:])

	req.Header.Set("x-amz-date", amzDate)
	req.Header.Set("x-amz-content-sha256", payloadHash)

	// Build Canonical Headers (must be lowercase and sorted alphabetically)
	host := req.Header.Get("Host")
	cacheControl := req.Header.Get("Cache-Control")
	contentType := req.Header.Get("Content-Type")

	canonicalHeaders := fmt.Sprintf(
		"cache-control:%s\ncontent-type:%s\nhost:%s\nx-amz-content-sha256:%s\nx-amz-date:%s\n",
		cacheControl, contentType, host, payloadHash, amzDate,
	)
	signedHeaders := "cache-control;content-type;host;x-amz-content-sha256;x-amz-date"

	canonicalURI := req.URL.EscapedPath()
	if canonicalURI == "" {
		canonicalURI = "/"
	}

	canonicalRequest := strings.Join([]string{
		req.Method,
		canonicalURI,
		"", // query string (empty for simple PUT)
		canonicalHeaders,
		signedHeaders,
		payloadHash,
	}, "\n")

	canonicalRequestHash := sha256Hex([]byte(canonicalRequest))

	credentialScope := fmt.Sprintf("%s/%s/%s/aws4_request", dateStamp, region, service)
	stringToSign := strings.Join([]string{
		"AWS4-HMAC-SHA256",
		amzDate,
		credentialScope,
		canonicalRequestHash,
	}, "\n")

	signingKey := getSignatureKey(secretKey, dateStamp, region, service)
	signature := hex.EncodeToString(hmacSHA256(signingKey, []byte(stringToSign)))

	authHeader := fmt.Sprintf(
		"AWS4-HMAC-SHA256 Credential=%s/%s, SignedHeaders=%s, Signature=%s",
		accessKey, credentialScope, signedHeaders, signature,
	)
	req.Header.Set("Authorization", authHeader)
}

func hmacSHA256(key []byte, data []byte) []byte {
	h := hmac.New(sha256.New, key)
	h.Write(data)
	return h.Sum(nil)
}

func sha256Hex(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func getSignatureKey(key, dateStamp, regionName, serviceName string) []byte {
	kDate := hmacSHA256([]byte("AWS4"+key), []byte(dateStamp))
	kRegion := hmacSHA256(kDate, []byte(regionName))
	kService := hmacSHA256(kRegion, []byte(serviceName))
	kSigning := hmacSHA256(kService, []byte("aws4_request"))
	return kSigning
}
