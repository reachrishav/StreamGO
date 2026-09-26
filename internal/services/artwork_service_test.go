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

func TestArtworkService_ExtractID3Picture_PNG_Syncsafe(t *testing.T) {
	// 1. Construct PNG payload that contains \xff\xd8\xff in its body
	pngPayload := []byte("\x89PNG\r\n\x1a\n")
	pngPayload = append(pngPayload, []byte("fake_chunk_data_with_\xff\xd8\xff_embedded_in_stream")...)
	pngPayload = append(pngPayload, []byte("IEND\xae\x42\x60\x82")...)

	// APIC payload: [encoding: 1B] [mime: null-terminated] [picType: 1B] [desc: null-terminated] [image data]
	var apicPayload bytes.Buffer
	apicPayload.WriteByte(0) // ISO-8859-1
	apicPayload.WriteString("image/png\x00")
	apicPayload.WriteByte(3) // Cover (front)
	apicPayload.WriteString("Cover Art\x00")
	apicPayload.Write(pngPayload)

	payloadBytes := apicPayload.Bytes()
	frameLen := len(payloadBytes)

	// In ID3v2.4, frame size is syncsafe (7 bits per byte)
	syncsafeLen := []byte{
		byte((frameLen >> 21) & 0x7F),
		byte((frameLen >> 14) & 0x7F),
		byte((frameLen >> 7) & 0x7F),
		byte(frameLen & 0x7F),
	}

	var tag bytes.Buffer
	// ID3v2.4 Header (10 bytes)
	tag.WriteString("ID3")
	tag.WriteByte(4) // v2.4
	tag.WriteByte(0) // revision
	tag.WriteByte(0) // flags
	tag.Write([]byte{0, 0, 0x10, 0}) // tag size syncsafe

	// APIC Frame Header (10 bytes)
	tag.WriteString("APIC")
	tag.Write(syncsafeLen)
	tag.Write([]byte{0, 0}) // flags
	tag.Write(payloadBytes)

	// Trailing audio / frame data
	tag.WriteString("TRAILING_AUDIO_DATA_OR_NEXT_FRAME")

	extracted, mime, err := ExtractID3Picture(tag.Bytes())
	if err != nil {
		t.Fatalf("ExtractID3Picture failed: %v", err)
	}
	if mime != "image/png" {
		t.Fatalf("Expected image/png, got %s", mime)
	}
	if !bytes.Equal(extracted, pngPayload) {
		t.Fatalf("Extracted payload did not match expected PNG (len %d vs %d)", len(extracted), len(pngPayload))
	}
}

func TestArtworkService_ExtractID3Picture_JPEG_V23(t *testing.T) {
	testJPEG := createTestJPEG(50, 50)

	var apicPayload bytes.Buffer
	apicPayload.WriteByte(0)
	apicPayload.WriteString("image/jpeg\x00")
	apicPayload.WriteByte(3)
	apicPayload.WriteString("Cover\x00")
	apicPayload.Write(testJPEG)

	payloadBytes := apicPayload.Bytes()
	frameLen := uint32(len(payloadBytes))

	var tag bytes.Buffer
	// ID3v2.3 Header (10 bytes)
	tag.WriteString("ID3")
	tag.WriteByte(3) // v2.3
	tag.WriteByte(0)
	tag.WriteByte(0)
	tag.Write([]byte{0, 0, 0x10, 0})

	// APIC Frame Header (10 bytes) - standard 32-bit big endian length
	tag.WriteString("APIC")
	var lenBuf [4]byte
	binary.BigEndian.PutUint32(lenBuf[:], frameLen)
	tag.Write(lenBuf[:])
	tag.Write([]byte{0, 0})
	tag.Write(payloadBytes)
	tag.WriteString("TRAILING_MP3_AUDIO_STREAM_BYTES")

	extracted, mime, err := ExtractID3Picture(tag.Bytes())
	if err != nil {
		t.Fatalf("ExtractID3Picture failed: %v", err)
	}
	if mime != "image/jpeg" {
		t.Fatalf("Expected image/jpeg, got %s", mime)
	}
	if !bytes.Equal(extracted, testJPEG) {
		t.Fatalf("Extracted JPEG did not match expected (len %d vs %d)", len(extracted), len(testJPEG))
	}
}
