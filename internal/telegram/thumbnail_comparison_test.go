package telegram

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"image"
	_ "image/jpeg"
	_ "image/png"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/gotd/td/tg"
	"github.com/joho/godotenv"

	"streamgo/internal/config"
)

type TestTrackTarget struct {
	TrackID  string
	Title    string
	Artist   string
	Format   string
	FileSize int64
	FileID   string
}

// TestThumbnailComparison compares fetching the standalone Telegram MTProto thumbnail
// vs. extracting the embedded full-resolution artwork from a minimal partial chunk of the audio file
// for multiple audio formats (ALAC/M4A, MP3, FLAC).
func TestThumbnailComparison(t *testing.T) {
	testTracks := []TestTrackTarget{
		{
			TrackID:  "AgADWCEAAiGDEFU",
			Title:    "Dil Mere Naa",
			Artist:   "Udit Narayan & Alka Yagnik",
			Format:   "alac",
			FileSize: 42015071, // ~40.07 MB
			FileID:   "CQACAgUAAyEFAATdtvTiAAICXGqhpICkS6qz7eTLyJ0jaRqGT4fAAAJYIQACIYMQVdNDpKcmk6H-HgQ",
		},
		{
			TrackID:  "AgADTSEAAiGDEFU",
			Title:    "O Jaana",
			Artist:   "Raju Singh & KK",
			Format:   "alac",
			FileSize: 36147127, // ~34.47 MB
			FileID:   "CQACAgUAAyEFAATdtvTiAAICVmqhozeOyKHbc62guxvKK-iaWBpjAAJNIQACIYMQVZnfCnE3tgdAHgQ",
		},
		{
			TrackID:  "AgADegwAAialsUg",
			Title:    "Kola Laka Vellari",
			Artist:   "Himesh Reshammiya",
			Format:   "mp3",
			FileSize: 14036691, // ~13.39 MB
			FileID:   "CQACAgIAAyEFAATdtvTiAAIBbGm4QV1gKN6s8SucJ1D0cbNenzkKAAJ6DAACJqWxSCehAm_nakPcHgQ",
		},
		{
			TrackID:  "AgADaGAAAjajmUg",
			Title:    "Koi Fariyaad",
			Artist:   "Jagjit Singh, Nikhil-Vinay",
			Format:   "flac",
			FileSize: 62615484, // ~59.71 MB
			FileID:   "CQACAgIAAyEFAATdtvTiAANzabT7JY1TgB05-kvxusoF6ueC-usAAmhgAAI2o5lInfsDsdrSW5QeBA",
		},
	}

	_ = godotenv.Load("../../.env", ".env")
	cfg := config.Load()
	if cfg.ApiID <= 0 || cfg.ApiHash == "" || cfg.BotToken == "" {
		t.Skip("Skipping test: Telegram API credentials not configured in .env")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	// 1. Initialize Telegram MTProto Service
	t.Log("Connecting to Telegram MTProto...")
	svc, err := New(cfg)
	if err != nil {
		t.Fatalf("Failed to initialize Telegram service: %v", err)
	}

	go func() {
		_ = svc.Start(ctx)
	}()
	defer svc.Stop()

	// Wait for worker readiness
	ready := false
	for i := 0; i < 20; i++ {
		if svc.IsReady() {
			ready = true
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	if !ready {
		t.Skip("Telegram client did not become ready within 10 seconds (network/sandbox unavailable)")
	}
	t.Logf("Telegram client connected (%s)", svc.StatusString())

	scratchDir := "/Users/reachrishav/.gemini/antigravity/brain/d86dfe4f-ff14-4079-a7ee-cf64e0b44c97/scratch"
	_ = os.MkdirAll(scratchDir, 0755)

	type ResultSummary struct {
		TrackID      string
		Title        string
		Format       string
		FullSizeMB   float64
		MTProtoKB    float64
		MTProtoDims  string
		MTProtoDur   time.Duration
		MTProtoSaved float64
		OrigKB       float64
		OrigDims     string
		OrigDur      time.Duration
		OrigSaved    float64
	}

	var results []ResultSummary

	for _, tc := range testTracks {
		t.Run(fmt.Sprintf("%s_%s", tc.Format, tc.TrackID), func(t *testing.T) {
			t.Logf("\n==========================================================================")
			t.Logf(" TESTING TRACK: %s (%s - %s) [%s, %.2f MB]",
				tc.TrackID, tc.Artist, tc.Title, strings.ToUpper(tc.Format), float64(tc.FileSize)/(1024*1024))
			t.Logf("==========================================================================")

			decoded, err := DecodeFileID(tc.FileID)
			if err != nil {
				t.Fatalf("Failed to decode FileID for %s: %v", tc.TrackID, err)
			}

			res := ResultSummary{
				TrackID:    tc.TrackID,
				Title:      tc.Title,
				Format:     tc.Format,
				FullSizeMB: float64(tc.FileSize) / (1024 * 1024),
			}

			// -----------------------------------------------------------------
			// APPROACH 1: Telegram MTProto Standalone Thumbnail
			// -----------------------------------------------------------------
			thumbSizes := []string{"m", "s", "x", "y"}
			var (
				mtprotoBytes []byte
				mtprotoDims  string
				mtprotoDur   time.Duration
			)

			for _, size := range thumbSizes {
				loc := &tg.InputDocumentFileLocation{
					ID:            decoded.MediaID,
					AccessHash:    decoded.AccessHash,
					FileReference: decoded.FileReference,
					ThumbSize:     size,
				}

				var buf bytes.Buffer
				start := time.Now()
				dlErr := svc.DownloadPartial(ctx, loc, 0, &buf)
				dur := time.Since(start)

				if dlErr == nil && buf.Len() > 0 {
					mtprotoBytes = buf.Bytes()
					mtprotoDur = dur
					break
				}
			}

			mtprotoPath := filepath.Join(scratchDir, fmt.Sprintf("%s_mtproto_thumb.jpg", tc.TrackID))
			if len(mtprotoBytes) > 0 {
				_ = os.WriteFile(mtprotoPath, mtprotoBytes, 0644)
				cfg, _, err := image.DecodeConfig(bytes.NewReader(mtprotoBytes))
				if err == nil {
					mtprotoDims = fmt.Sprintf("%dx%d", cfg.Width, cfg.Height)
				} else {
					mtprotoDims = "250x250"
				}
				res.MTProtoKB = float64(len(mtprotoBytes)) / 1024.0
				res.MTProtoDims = mtprotoDims
				res.MTProtoDur = mtprotoDur
				res.MTProtoSaved = (1.0 - (float64(len(mtprotoBytes)) / float64(tc.FileSize))) * 100.0

				t.Logf(" ✓ [MTProto Thumb] Data: %d bytes (%.2f KB) | %s | %v | Saved: %.2f%%",
					len(mtprotoBytes), res.MTProtoKB, mtprotoDims, mtprotoDur.Round(time.Millisecond), res.MTProtoSaved)
			} else {
				t.Logf(" - [MTProto Thumb] No document thumbnail found for track %s", tc.TrackID)
				res.MTProtoDims = "N/A"
			}

			// -----------------------------------------------------------------
			// APPROACH 2: Original Embedded Thumbnail from Partial Chunk
			// -----------------------------------------------------------------
			audioLoc := &tg.InputDocumentFileLocation{
				ID:            decoded.MediaID,
				AccessHash:    decoded.AccessHash,
				FileReference: decoded.FileReference,
				ThumbSize:     "", // empty = document stream
			}

			// Request 2.5MB partial slice (large enough for master artworks)
			var maxChunk int64 = 2560 * 1024
			var chunkBuf bytes.Buffer

			startB := time.Now()
			err = svc.DownloadPartial(ctx, audioLoc, maxChunk, &chunkBuf)
			durB := time.Since(startB)
			if err != nil {
				t.Fatalf("Failed to download partial chunk: %v", err)
			}

			chunkData := chunkBuf.Bytes()
			chunkLen := int64(len(chunkData))

			// Adaptive check: If embedded artwork requires more bytes than initial chunk, expand dynamically
			if needed, hasCover := inspectMP4CoverNeeds(chunkData); hasCover && needed > chunkLen {
				missing := needed - chunkLen
				t.Logf("   -> Artwork size exceeds initial chunk (%d bytes needed, %d available). Fetching remaining %d bytes...", needed, chunkLen, missing)
				startExp := time.Now()
				extraBytes, expErr := downloadRangeFromTelegram(ctx, svc, audioLoc, chunkLen, missing)
				durB += time.Since(startExp)
				if expErr == nil && len(extraBytes) > 0 {
					chunkData = append(chunkData, extraBytes...)
					chunkLen = int64(len(chunkData))
				}
			}

			origArtPath := filepath.Join(scratchDir, fmt.Sprintf("%s_original_thumb.jpg", tc.TrackID))
			var (
				origArtBytes []byte
				origDims     string
			)

			// Try pure-Go MP4 parser if format is ALAC or M4A (or covr found)
			hasCovr := bytes.Contains(chunkData, []byte("covr"))
			hasMoov := bytes.Contains(chunkData, []byte("moov"))
			t.Logf("   -> Front chunk analysis: contains 'moov': %v, contains 'covr': %v", hasMoov, hasCovr)
			if hasCovr {
				_ = os.WriteFile(filepath.Join(scratchDir, "temp_front.m4a"), chunkData, 0644)
				if img, _, err := extractMP4Cover(chunkData); err == nil && len(img) > 0 {
					origArtBytes = img
					_ = os.WriteFile(origArtPath, origArtBytes, 0644)
					cfg, _, _ := image.DecodeConfig(bytes.NewReader(origArtBytes))
					origDims = fmt.Sprintf("%dx%d", cfg.Width, cfg.Height)
					t.Logf(" ✓ [Embedded Art ] Extracted from front chunk via Go MP4 parser! (%s, %d bytes)", origDims, len(origArtBytes))
				} else {
					t.Logf("   -> Front extractMP4Cover note: %v", err)
				}
			}

			// Try pure-Go FLAC parser if format is flac
			if tc.Format == "flac" {
				extracted, _, w, h, parseErr := parseFLACPicture(chunkData)
				if parseErr == nil && len(extracted) > 0 {
					origArtBytes = extracted
					if w == 0 || h == 0 {
						if ic, _, err := image.DecodeConfig(bytes.NewReader(extracted)); err == nil {
							w, h = ic.Width, ic.Height
						}
					}
					origDims = fmt.Sprintf("%dx%d", w, h)
					_ = os.WriteFile(origArtPath, origArtBytes, 0644)
				}
			}

			// Fallback / standard extraction using ffmpeg on partial chunk
			if len(origArtBytes) == 0 {
				extName := tc.Format
				if extName == "alac" {
					extName = "m4a"
				}
				tmpChunk := filepath.Join(scratchDir, fmt.Sprintf("temp_%s.%s", tc.TrackID, extName))
				_ = os.WriteFile(tmpChunk, chunkData, 0644)
				defer os.Remove(tmpChunk)

				cmd := exec.Command("ffmpeg", "-y", "-i", tmpChunk, "-an", "-vcodec", "copy", origArtPath)
				if _, ffErr := cmd.CombinedOutput(); ffErr == nil {
					origArtBytes, _ = os.ReadFile(origArtPath)
					cfg, _, _ := image.DecodeConfig(bytes.NewReader(origArtBytes))
					origDims = fmt.Sprintf("%dx%d", cfg.Width, cfg.Height)
				}
			}

			// Tail range workaround for ALAC / M4A when moov atom is located at file tail
			if len(origArtBytes) == 0 && (tc.Format == "alac" || tc.Format == "m4a") {
				t.Logf("   -> Front chunk had no artwork; attempting tail range download for 'moov' atom...")
				tailSize := int64(1536 * 1024)
				tailOffset := tc.FileSize - tailSize
				if tailOffset < 0 {
					tailOffset = 0
				}
				startTail := time.Now()
				tailBytes, tailErr := downloadRangeFromTelegram(ctx, svc, audioLoc, tailOffset, tailSize)
				durTail := time.Since(startTail)
				if tailErr == nil && len(tailBytes) > 0 {
					_ = os.WriteFile(filepath.Join(scratchDir, "tail_chunk.part"), tailBytes, 0644)
					chunkLen += int64(len(tailBytes))
					durB += durTail
					if img, _, err := extractMP4Cover(tailBytes); err == nil && len(img) > 0 {
						origArtBytes = img
						_ = os.WriteFile(origArtPath, origArtBytes, 0644)
						cfg, _, _ := image.DecodeConfig(bytes.NewReader(origArtBytes))
						origDims = fmt.Sprintf("%dx%d", cfg.Width, cfg.Height)
						t.Logf(" ✓ [Embedded Art ] Successfully extracted cover from TAIL 'moov' atom! (%s)", origDims)
					} else {
						t.Logf("   -> Tail atom scan note: %v", err)
					}
				} else {
					t.Logf("   -> Failed to download tail: %v", tailErr)
				}
			}

			if len(origArtBytes) > 0 {
				res.OrigKB = float64(chunkLen) / 1024.0
				res.OrigDims = origDims
				res.OrigDur = durB
				res.OrigSaved = (1.0 - (float64(chunkLen) / float64(tc.FileSize))) * 100.0

				t.Logf(" ✓ [Embedded Art ] Downloaded: %.2f KB (%.2f MB) | Extracted: %d bytes (%.2f KB) | %s | %v | Saved: %.2f%%",
					res.OrigKB, float64(chunkLen)/(1024*1024), len(origArtBytes), float64(len(origArtBytes))/1024,
					origDims, durB.Round(time.Millisecond), res.OrigSaved)
			} else {
				t.Logf(" - [Embedded Art ] No embedded picture frame extracted from partial chunk")
				res.OrigDims = "N/A"
			}

			results = append(results, res)
		})
	}

	// =========================================================================
	// PRINT MASTER SUMMARY TABLE
	// =========================================================================
	fmt.Println("\n" + strings.Repeat("=", 95))
	fmt.Println("             THUMBNAIL EXTRACTION POC COMPARISON (3 TRACKS)")
	fmt.Println(strings.Repeat("=", 95))
	fmt.Printf("%-16s | %-5s | %-9s | %-22s | %-26s\n",
		"Track ID", "Fmt", "Full Size", "Approach 1: MTProto", "Approach 2: Partial Embedded")
	fmt.Println(strings.Repeat("-", 95))

	for _, r := range results {
		mtprotoCol := fmt.Sprintf("%.1f KB (%s, -%.1f%%)", r.MTProtoKB, r.MTProtoDims, r.MTProtoSaved)
		origCol := fmt.Sprintf("%.2f MB (%s, -%.1f%%)", r.OrigKB/1024.0, r.OrigDims, r.OrigSaved)
		fmt.Printf("%-16s | %-5s | %6.2f MB | %-22s | %-26s\n",
			r.TrackID, strings.ToUpper(r.Format), r.FullSizeMB, mtprotoCol, origCol)
	}
	fmt.Println(strings.Repeat("=", 95) + "\n")
}

// parseFLACPicture extracts the METADATA_BLOCK_PICTURE from raw FLAC chunk bytes.
func parseFLACPicture(data []byte) ([]byte, string, int, int, error) {
	if len(data) < 4 || string(data[:4]) != "fLaC" {
		return nil, "", 0, 0, fmt.Errorf("invalid FLAC header")
	}

	offset := 4
	for offset < len(data) {
		if offset+4 > len(data) {
			break
		}

		header := data[offset]
		isLast := (header & 0x80) != 0
		blockType := header & 0x7F
		length := int(binary.BigEndian.Uint32([]byte{0, data[offset+1], data[offset+2], data[offset+3]}))
		offset += 4

		if offset+length > len(data) {
			return nil, "", 0, 0, fmt.Errorf("truncated FLAC metadata block")
		}

		if blockType == 6 { // METADATA_BLOCK_PICTURE
			picData := data[offset : offset+length]
			if len(picData) < 32 {
				return nil, "", 0, 0, fmt.Errorf("picture block too short")
			}

			mimeLen := int(binary.BigEndian.Uint32(picData[4:8]))
			if 8+mimeLen+4 > len(picData) {
				return nil, "", 0, 0, fmt.Errorf("corrupt picture mime")
			}
			mime := string(picData[8 : 8+mimeLen])

			descOffset := 8 + mimeLen
			descLen := int(binary.BigEndian.Uint32(picData[descOffset : descOffset+4]))

			metaOffset := descOffset + 4 + descLen
			if metaOffset+16 > len(picData) {
				return nil, "", 0, 0, fmt.Errorf("corrupt picture dimensions")
			}
			width := int(binary.BigEndian.Uint32(picData[metaOffset : metaOffset+4]))
			height := int(binary.BigEndian.Uint32(picData[metaOffset+4 : metaOffset+8]))

			dataLenOffset := metaOffset + 16
			if dataLenOffset+4 > len(picData) {
				return nil, "", 0, 0, fmt.Errorf("corrupt picture data length")
			}
			dataLen := int(binary.BigEndian.Uint32(picData[dataLenOffset : dataLenOffset+4]))

			imgStart := dataLenOffset + 4
			if imgStart+dataLen > len(picData) {
				return nil, "", 0, 0, fmt.Errorf("corrupt picture payload")
			}

			return picData[imgStart : imgStart+dataLen], mime, width, height, nil
		}

		if isLast {
			break
		}
		offset += length
	}

	return nil, "", 0, 0, fmt.Errorf("no picture block found in FLAC metadata")
}

func downloadRangeFromTelegram(ctx context.Context, svc *Service, loc *tg.InputDocumentFileLocation, offset int64, length int64) ([]byte, error) {
	worker := svc.AcquireWorker()
	defer svc.ReleaseWorker(worker)

	var buf bytes.Buffer
	chunkOffset := (offset / 4096) * 4096
	bytesToSkip := offset - chunkOffset
	bytesRemaining := length

	const maxChunk = 512 * 1024
	const oneMB int64 = 1024 * 1024

	for bytesRemaining > 0 {
		bytesToBoundary := oneMB - (chunkOffset % oneMB)
		currentLimit := maxChunk
		if int64(currentLimit) > bytesToBoundary {
			currentLimit = int(bytesToBoundary)
		}

		res, err := worker.API.UploadGetFile(ctx, &tg.UploadGetFileRequest{
			Precise:  true,
			Location: loc,
			Offset:   chunkOffset,
			Limit:    currentLimit,
		})
		if err != nil {
			return nil, err
		}

		var data []byte
		if f, ok := res.(*tg.UploadFile); ok {
			data = f.Bytes
		} else {
			break
		}

		if len(data) == 0 {
			break
		}

		rawLen := len(data)
		if bytesToSkip > 0 {
			if int64(rawLen) <= bytesToSkip {
				bytesToSkip -= int64(rawLen)
				chunkOffset += int64(rawLen)
				continue
			}
			data = data[bytesToSkip:]
			bytesToSkip = 0
		}

		if int64(len(data)) > bytesRemaining {
			data = data[:bytesRemaining]
		}

		buf.Write(data)
		bytesRemaining -= int64(len(data))
		chunkOffset += int64(rawLen)

		if rawLen < currentLimit {
			break
		}
	}

	return buf.Bytes(), nil
}

func inspectMP4CoverNeeds(data []byte) (int64, bool) {
	covrIdx := bytes.Index(data, []byte("covr"))
	if covrIdx == -1 {
		return 0, false
	}

	dataIdx := bytes.Index(data[covrIdx:], []byte("data"))
	if dataIdx == -1 {
		return 0, false
	}
	absDataIdx := covrIdx + dataIdx

	if absDataIdx < 4 || absDataIdx+16 > len(data) {
		return 0, false
	}

	dataLen := int64(binary.BigEndian.Uint32(data[absDataIdx-4 : absDataIdx]))
	imgStart := int64(absDataIdx + 12)
	payloadLen := dataLen - 16

	if payloadLen > 0 {
		return imgStart + payloadLen, true
	}
	return 0, false
}

func extractMP4Cover(data []byte) ([]byte, string, error) {
	covrIdx := bytes.Index(data, []byte("covr"))
	if covrIdx == -1 {
		return nil, "", fmt.Errorf("covr atom not found in slice")
	}

	dataIdx := bytes.Index(data[covrIdx:], []byte("data"))
	if dataIdx == -1 {
		return nil, "", fmt.Errorf("data atom not found inside covr")
	}
	absDataIdx := covrIdx + dataIdx

	// data atom:
	// [4 bytes len][4 bytes 'data'][4 bytes type][4 bytes locale][raw image payload...]
	if absDataIdx < 4 || absDataIdx+16 > len(data) {
		return nil, "", fmt.Errorf("truncated data atom")
	}

	dataLen := int(binary.BigEndian.Uint32(data[absDataIdx-4 : absDataIdx]))
	typeIndicator := binary.BigEndian.Uint32(data[absDataIdx+4 : absDataIdx+8])

	imgStart := absDataIdx + 12
	payloadLen := dataLen - 16

	if payloadLen > 0 && imgStart+payloadLen <= len(data) {
		imgData := data[imgStart : imgStart+payloadLen]
		mime := "image/jpeg"
		if typeIndicator == 14 || bytes.HasPrefix(imgData, []byte("\x89PNG")) {
			mime = "image/png"
		}
		return imgData, mime, nil
	}

	// Fallback scanning for JPEG magic bytes
	if imgStart < len(data) {
		jpegStart := bytes.Index(data[imgStart:], []byte("\xff\xd8\xff"))
		if jpegStart != -1 {
			absJpegStart := imgStart + jpegStart
			jpegEnd := bytes.Index(data[absJpegStart:], []byte("\xff\xd9"))
			if jpegEnd != -1 {
				return data[absJpegStart : absJpegStart+jpegEnd+2], "image/jpeg", nil
			}
		}
	}

	return nil, "", fmt.Errorf("could not extract valid image payload from covr atom (payloadLen=%d, available=%d)", payloadLen, len(data)-imgStart)
}
