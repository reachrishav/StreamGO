package services

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"os/exec"
	"strings"
	"time"

	"streamgo/internal/logger"
)

var logArtwork = logger.New("artwork")

// ArtworkService extracts and compresses album artwork from audio file bytes.
type ArtworkService struct{}

// NewArtworkService creates a new ArtworkService.
func NewArtworkService() *ArtworkService {
	return &ArtworkService{}
}

// ExtractArtwork attempts to extract embedded album artwork from an audio chunk.
// It detects container formats (MP4/ALAC/M4A, FLAC, ID3/MP3) automatically or via hint.
func (s *ArtworkService) ExtractArtwork(data []byte, formatHint string) ([]byte, string, error) {
	if len(data) == 0 {
		return nil, "", fmt.Errorf("empty audio data")
	}

	hint := strings.ToLower(strings.TrimSpace(formatHint))

	// 1. FLAC Container
	if hint == "flac" || (len(data) >= 4 && string(data[:4]) == "fLaC") {
		img, mime, _, _, err := ExtractFLACPicture(data)
		if err == nil && len(img) > 0 {
			return img, mime, nil
		}
	}

	// 2. MP4 / ALAC / M4A Container (inspect for covr or moov)
	if hint == "alac" || hint == "m4a" || bytes.Contains(data, []byte("covr")) || bytes.Contains(data, []byte("moov")) {
		img, mime, err := ExtractMP4Cover(data)
		if err == nil && len(img) > 0 {
			return img, mime, nil
		}
	}

	// 3. ID3v2 (MP3, AAC)
	if hint == "mp3" || (len(data) >= 3 && string(data[:3]) == "ID3") || bytes.Contains(data, []byte("APIC")) {
		img, mime, err := ExtractID3Picture(data)
		if err == nil && len(img) > 0 {
			return img, mime, nil
		}
	}

	return nil, "", fmt.Errorf("no embedded artwork found in audio chunk")
}

// CompressToWebP compresses and resizes raw image bytes into WebP format using an in-memory FFmpeg pipe.
// maxDim sets maximum width/height (aspect ratio preserved). quality is 0-100 (default 80).
func (s *ArtworkService) CompressToWebP(ctx context.Context, raw []byte, maxDim int, quality int) ([]byte, error) {
	if len(raw) == 0 {
		return nil, fmt.Errorf("empty image bytes")
	}
	if maxDim <= 0 {
		maxDim = 1000
	}
	if quality <= 0 || quality > 100 {
		quality = 80
	}

	start := time.Now()
	args := []string{
		"-hide_banner",
		"-loglevel", "error",
		"-i", "pipe:0",
		"-vf", fmt.Sprintf("scale='min(%d,iw)':-1", maxDim),
		"-c:v", "libwebp",
		"-quality", fmt.Sprintf("%d", quality),
		"-f", "webp",
		"pipe:1",
	}

	cmd := exec.CommandContext(ctx, "ffmpeg", args...)
	cmd.Stdin = bytes.NewReader(raw)
	var out bytes.Buffer
	cmd.Stdout = &out
	var errBuf bytes.Buffer
	cmd.Stderr = &errBuf

	if err := cmd.Run(); err != nil {
		return nil, fmt.Errorf("ffmpeg webp compression failed: %w: %s", err, errBuf.String())
	}

	dur := time.Since(start)
	logArtwork.Debugf("Compressed artwork to WebP: %d bytes -> %d bytes in %v",
		len(raw), out.Len(), dur.Round(time.Millisecond))

	return out.Bytes(), nil
}

// ExtractMP4Cover extracts embedded cover art from MP4 / M4A / ALAC atom structure (ilst -> covr -> data).
func ExtractMP4Cover(data []byte) ([]byte, string, error) {
	covrIdx := bytes.Index(data, []byte("covr"))
	if covrIdx == -1 {
		return nil, "", fmt.Errorf("covr atom not found in slice")
	}

	dataIdx := bytes.Index(data[covrIdx:], []byte("data"))
	if dataIdx == -1 {
		return nil, "", fmt.Errorf("data atom not found inside covr")
	}
	absDataIdx := covrIdx + dataIdx

	if absDataIdx < 4 || absDataIdx+16 > len(data) {
		return nil, "", fmt.Errorf("truncated data atom header")
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

	// Fallback scanning for JPEG magic markers if atom length was slightly off
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

// InspectMP4CoverNeeds determines the total byte length required from file start to capture the entire cover atom.
func InspectMP4CoverNeeds(data []byte) (int64, bool) {
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

// ExtractFLACPicture extracts the METADATA_BLOCK_PICTURE from raw FLAC chunk bytes.
func ExtractFLACPicture(data []byte) ([]byte, string, int, int, error) {
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

// ExtractID3Picture extracts the APIC (attached picture) frame from ID3v2 tag bytes.
func ExtractID3Picture(data []byte) ([]byte, string, error) {
	apicIdx := bytes.Index(data, []byte("APIC"))
	if apicIdx == -1 {
		return nil, "", fmt.Errorf("APIC frame not found")
	}

	// APIC frame header is 10 bytes:
	// 'APIC' (4) + Size (4) + Flags (2)
	frameHeader := data[apicIdx:]
	if len(frameHeader) < 10 {
		return nil, "", fmt.Errorf("truncated APIC header")
	}

	frameSize := int(binary.BigEndian.Uint32(frameHeader[4:8]))
	if apicIdx+10+frameSize > len(data) {
		// Attempt to scan for JPEG magic within available slice
		payload := frameHeader[10:]
		if jpegStart := bytes.Index(payload, []byte("\xff\xd8\xff")); jpegStart != -1 {
			if jpegEnd := bytes.Index(payload[jpegStart:], []byte("\xff\xd9")); jpegEnd != -1 {
				return payload[jpegStart : jpegStart+jpegEnd+2], "image/jpeg", nil
			}
		}
		return nil, "", fmt.Errorf("APIC frame payload truncated")
	}

	payload := frameHeader[10 : 10+frameSize]
	if len(payload) < 4 {
		return nil, "", fmt.Errorf("APIC payload too short")
	}

	// Payload layout:
	// [1 byte text encoding]
	// [MIME type string (null-terminated)]
	// [1 byte picture type]
	// [Description string (null-terminated)]
	// [Raw image bytes...]
	mimeEnd := bytes.IndexByte(payload[1:], 0)
	if mimeEnd == -1 {
		return nil, "", fmt.Errorf("corrupt APIC mime terminator")
	}
	mime := string(payload[1 : 1+mimeEnd])
	if mime == "image/jpg" {
		mime = "image/jpeg"
	}

	// Search for image magic bytes (JPEG \xff\xd8\xff or PNG \x89PNG)
	jpegStart := bytes.Index(payload, []byte("\xff\xd8\xff"))
	if jpegStart != -1 {
		return payload[jpegStart:], "image/jpeg", nil
	}
	pngStart := bytes.Index(payload, []byte("\x89PNG"))
	if pngStart != -1 {
		return payload[pngStart:], "image/png", nil
	}

	return nil, "", fmt.Errorf("could not locate image magic bytes in APIC frame")
}
