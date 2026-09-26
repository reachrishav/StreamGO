package services

import (
	"bytes"
	"context"
	"encoding/binary"
	"image"
	"image/color"
	"image/jpeg"
	"testing"
)

func createTestJPEG(width, height int) []byte {
	img := image.NewRGBA(image.Rect(0, 0, width, height))
	for y := 0; y < height; y++ {
		for x := 0; x < width; x++ {
			img.Set(x, y, color.RGBA{R: 200, G: 100, B: 50, A: 255})
		}
	}
	var buf bytes.Buffer
	_ = jpeg.Encode(&buf, img, &jpeg.Options{Quality: 80})
	return buf.Bytes()
}

func createTestMP4CovrChunk(imgBytes []byte) []byte {
	var buf bytes.Buffer
	buf.WriteString("ftypM4A ")
	buf.WriteString("moov")
	buf.WriteString("udta")
	buf.WriteString("meta")
	buf.WriteString("ilst")

	// covr atom
	covrStart := buf.Len()
	buf.Write([]byte{0, 0, 0, 0}) // placeholder for covr size
	buf.WriteString("covr")

	// data atom
	dataStart := buf.Len()
	buf.Write([]byte{0, 0, 0, 0}) // placeholder for data size
	buf.WriteString("data")
	buf.Write([]byte{0, 0, 0, 13}) // type 13 = jpeg
	buf.Write([]byte{0, 0, 0, 0})  // locale 0
	buf.Write(imgBytes)

	dataLen := uint32(buf.Len() - dataStart)
	binary.BigEndian.PutUint32(buf.Bytes()[dataStart:dataStart+4], dataLen)

	covrLen := uint32(buf.Len() - covrStart)
	binary.BigEndian.PutUint32(buf.Bytes()[covrStart:covrStart+4], covrLen)

	return buf.Bytes()
}

func TestArtworkService_ExtractMP4Cover(t *testing.T) {
	testImg := createTestJPEG(100, 100)
	chunk := createTestMP4CovrChunk(testImg)

	svc := NewArtworkService()
	extracted, mime, err := svc.ExtractArtwork(chunk, "alac")
	if err != nil {
		t.Fatalf("Failed to extract MP4 cover: %v", err)
	}
	if mime != "image/jpeg" {
		t.Fatalf("Expected image/jpeg mime, got %s", mime)
	}
	if len(extracted) != len(testImg) {
		t.Fatalf("Extracted size %d != expected %d", len(extracted), len(testImg))
	}
}

func TestArtworkService_CompressToWebP(t *testing.T) {
	testImg := createTestJPEG(500, 500)
	svc := NewArtworkService()

	webpBytes, err := svc.CompressToWebP(context.Background(), testImg, 300, 80)
	if err != nil {
		t.Fatalf("CompressToWebP failed: %v", err)
	}
	if len(webpBytes) == 0 {
		t.Fatal("Expected non-empty webp output")
	}
	if !bytes.HasPrefix(webpBytes, []byte("RIFF")) || !bytes.Contains(webpBytes[:16], []byte("WEBP")) {
		t.Fatal("Output does not have valid WebP magic header")
	}
}
