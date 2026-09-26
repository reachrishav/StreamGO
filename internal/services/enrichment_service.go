package services

import (
	"context"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"streamgo/internal/database"
	"streamgo/internal/logger"
	"streamgo/internal/models"
)

var logEnrich = logger.New("enrichment")

// MediaDownloader downloads partial media bytes for an indexed file_id or track.
type MediaDownloader interface {
	DownloadPartialByFileID(ctx context.Context, fileID string, maxBytes int64, w io.Writer) error
	DownloadPartialForTrack(ctx context.Context, track *models.Track, maxBytes int64, w io.Writer, onRefreshed func(botID, fileID string)) error
}

// EnrichmentService runs background worker goroutines to enrich track metadata.
type EnrichmentService struct {
	tracksCol  *mongo.Collection
	artistsCol *mongo.Collection
	albumsCol  *mongo.Collection

	coverSearch *CoverSearchService
	lyricsSvc   *LyricsEnrichmentService
	downloader  MediaDownloader

	workQueue chan string
	inFlight  sync.Map
	wg        sync.WaitGroup
}

// NewEnrichmentService creates a new EnrichmentService.
func NewEnrichmentService(
	db *database.Client,
	coverSearch *CoverSearchService,
	lyricsSvc *LyricsEnrichmentService,
) *EnrichmentService {
	var tracksCol, artistsCol, albumsCol *mongo.Collection
	if db != nil {
		tracksCol = db.Collection("audioTracks")
		artistsCol = db.Collection("artists")
		albumsCol = db.Collection("albums")
	}

	return &EnrichmentService{
		tracksCol:   tracksCol,
		artistsCol:  artistsCol,
		albumsCol:   albumsCol,
		coverSearch: coverSearch,
		lyricsSvc:   lyricsSvc,
		workQueue:   make(chan string, 500),
	}
}

// SetDownloader configures the MediaDownloader implementation for streaming partial media.
func (s *EnrichmentService) SetDownloader(d MediaDownloader) {
	s.downloader = d
}

// TriggerEnrich pushes a track ID to the priority enrichment queue.
func (s *EnrichmentService) TriggerEnrich(trackID string) {
	trackID = strings.TrimSpace(trackID)
	if trackID == "" {
		return
	}
	if _, loaded := s.inFlight.LoadOrStore(trackID, struct{}{}); loaded {
		return // already queued or currently being enriched
	}
	select {
	case s.workQueue <- trackID:
	default:
		s.inFlight.Delete(trackID)
		logEnrich.Warnf("enrichment queue full, dropping immediate trigger for %s", trackID)
	}
}

// Start launches the background worker goroutines and the periodic poller.
func (s *EnrichmentService) Start(ctx context.Context, numWorkers int) {
	if s.tracksCol == nil {
		return
	}
	if numWorkers <= 0 {
		numWorkers = 2
	}

	logEnrich.Infof("Starting background enrichment pool with %d workers", numWorkers)

	for i := 0; i < numWorkers; i++ {
		s.wg.Add(1)
		go s.workerLoop(ctx, i+1)
	}

	// Periodic polling loop finding unenriched tracks
	go s.pollLoop(ctx)
}

func (s *EnrichmentService) pollLoop(ctx context.Context) {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.enqueueUnenrichedTracks(ctx)
		}
	}
}

func (s *EnrichmentService) enqueueUnenrichedTracks(ctx context.Context) {
	staleThreshold := float64(time.Now().Unix() - 300) // 5 minutes
	filter := bson.M{
		"enriched": bson.M{"$ne": true},
		"deleted":  bson.M{"$ne": true},
		"$or": []bson.M{
			{"enriching": bson.M{"$ne": true}},
			{"enrichment_started_at": bson.M{"$lt": staleThreshold}},
			{"enrichment_started_at": bson.M{"$exists": false}},
		},
	}
	opts := options.Find().SetLimit(50).SetProjection(bson.M{"_id": 1})

	cursor, err := s.tracksCol.Find(ctx, filter, opts)
	if err != nil {
		return
	}
	defer cursor.Close(ctx)

	for cursor.Next(ctx) {
		var doc struct {
			ID string `bson:"_id"`
		}
		if err := cursor.Decode(&doc); err == nil && doc.ID != "" {
			if _, loaded := s.inFlight.LoadOrStore(doc.ID, struct{}{}); loaded {
				continue // already in flight
			}
			select {
			case s.workQueue <- doc.ID:
			default:
				s.inFlight.Delete(doc.ID)
				return
			}
		}
	}
}

func (s *EnrichmentService) workerLoop(ctx context.Context, workerID int) {
	defer s.wg.Done()

	for {
		select {
		case <-ctx.Done():
			return
		case trackID := <-s.workQueue:
			s.enrichSingleTrack(ctx, trackID)
		}
	}
}

func (s *EnrichmentService) enrichSingleTrack(ctx context.Context, trackID string) {
	defer s.inFlight.Delete(trackID)

	now := float64(time.Now().Unix())
	staleThreshold := now - 300

	// Atomically claim the track so no other worker or poller can process it concurrently
	claimFilter := bson.M{
		"_id":      trackID,
		"enriched": bson.M{"$ne": true},
		"deleted":  bson.M{"$ne": true},
		"$or": []bson.M{
			{"enriching": bson.M{"$ne": true}},
			{"enrichment_started_at": bson.M{"$lt": staleThreshold}},
			{"enrichment_started_at": bson.M{"$exists": false}},
		},
	}
	claimUpdate := bson.M{
		"$set": bson.M{
			"enriching":             true,
			"enrichment_started_at": now,
		},
	}

	var track models.Track
	err := s.tracksCol.FindOneAndUpdate(
		ctx,
		claimFilter,
		claimUpdate,
		options.FindOneAndUpdate().SetReturnDocument(options.After),
	).Decode(&track)
	if err != nil {
		// Already enriched, deleted, or claimed by another concurrent worker
		return
	}

	title := track.Audio.Title
	artist := track.EffectiveArtist()
	album := track.Audio.Album
	durationSec := track.Audio.DurationSec
	year := track.Audio.Year

	cleanTitle, cleanArtist := CleanMetadata(title, artist)
	if cleanTitle != title {
		title = cleanTitle
	}
	if cleanArtist != artist {
		artist = cleanArtist
	}

	nowTs := float64(time.Now().Unix())
	updateFields := bson.M{
		"enriched":    true,
		"enriched_at": nowTs,
		"updated_at":  nowTs,
	}
	if cleanTitle != track.Audio.Title {
		updateFields["audio.title"] = cleanTitle
	}
	if cleanArtist != track.Audio.Artist {
		updateFields["audio.artist"] = cleanArtist
		updateFields["audio.artists"] = SplitArtists(cleanArtist)
	}

	// 1. Partial Media Download & MediaInfo extraction (matching Python StreamXBot)
	if s.downloader != nil {
		tmpFile, err := os.CreateTemp("", fmt.Sprintf("streamgo_mi_%s_*.part", track.ID))
		if err == nil {
			tmpPath := tmpFile.Name()
			dlErr := s.downloader.DownloadPartialForTrack(ctx, &track, 2_000_000, tmpFile, func(botID, fileID string) {
				_ = s.tracksCol.FindOneAndUpdate(ctx, bson.M{"_id": trackID}, bson.M{
					"$set": bson.M{
						fmt.Sprintf("telegram.file_ids.%s", botID): fileID,
						"updated_at": float64(time.Now().Unix()),
					},
				})
			})
			tmpFile.Close()
			defer os.Remove(tmpPath)

			if dlErr == nil || dlErr == io.EOF {
				// A. Compute Content Hash (sha256 prefix of downloaded chunk)
				if ch, err := SHA256PrefixFile(tmpPath, 10*1024*1024); err == nil && ch != "" {
					updateFields["content_hash"] = ch
				}

				// B. Run MediaInfo
				if output, err := RunMediaInfo(tmpPath); err == nil && output != "" {
					meta := ParseMediaInfo(output, track.Audio.DurationSec, track.Telegram.FileSize)
					if meta != nil {
						if meta.Title != "" {
							title = meta.Title
							updateFields["audio.title"] = meta.Title
						}
						if meta.Artist != "" {
							artist = meta.Artist
							updateFields["audio.artist"] = meta.Artist
						}
						if len(meta.Artists) > 0 {
							updateFields["audio.artists"] = meta.Artists
						}
						if meta.Album != "" {
							album = meta.Album
							updateFields["audio.album"] = meta.Album
						}
						if meta.Composer != "" {
							updateFields["audio.composer"] = meta.Composer
						}
						if meta.Label != "" {
							updateFields["audio.label"] = meta.Label
						}
						if meta.Genre != "" {
							updateFields["audio.genre"] = meta.Genre
						}
						if meta.Year != nil {
							year = meta.Year
							updateFields["audio.year"] = *meta.Year
						}
						if meta.DurationSec > 0 {
							durationSec = meta.DurationSec
							updateFields["audio.duration_sec"] = meta.DurationSec
						}
						if meta.Type != "" {
							updateFields["audio.type"] = meta.Type
						}
						if meta.BitDepth != nil {
							updateFields["audio.bit_depth"] = *meta.BitDepth
						}
						if meta.BitrateKbps != nil {
							updateFields["audio.bitrate_kbps"] = *meta.BitrateKbps
						}
						if meta.SamplingRateHz != nil {
							updateFields["audio.sampling_rate_hz"] = *meta.SamplingRateHz
						}
						if meta.AlbumID != "" {
							updateFields["audio.album_id"] = meta.AlbumID
						} else if album != "" {
							updateFields["audio.album_id"] = GenerateAlbumID(album, year)
						}
					}
				} else if err != nil {
					logEnrich.Warnf("mediainfo failed on %s: %v", trackID, err)
				}
			} else {
				logEnrich.Warnf("partial download failed for track %s: %v", trackID, dlErr)
			}
		}
	}

	if title == "" {
		return
	}

	// 2. Calculate and set metadata fingerprint (will include resolved album)
	fp := BuildMetadataFingerprint(title, artist, album, durationSec)
	if fp != "" {
		updateFields["fingerprint"] = fp
	}

	// 3. Ensure album_id is set if album is present
	if album != "" && updateFields["audio.album_id"] == nil && track.Audio.AlbumID == "" {
		aid := GenerateAlbumID(album, year)
		if aid != "" {
			updateFields["audio.album_id"] = aid
		}
	}

	// 4. Fetch cover art if missing (stores in spotify, NEVER in audio)
	if track.Spotify.CoverURL == "" {
		coverURL, _, err := s.coverSearch.FindBestCover(ctx, title, artist, album)
		if err == nil && coverURL != "" {
			updateFields["spotify.cover_url"] = coverURL
			updateFields["spotify.big_cover_url"] = coverURL
			updateFields["spotify.cover_source"] = "itunes"
		}
	}

	// 5. Fetch artist avatar
	if artist != "" && track.Spotify.ArtistAvatar == "" {
		artistCover, _ := s.coverSearch.FindArtistAvatar(ctx, artist)
		if artistCover != "" {
			updateFields["spotify.artist_avatar"] = artistCover
		}
	}

	// 6. Fetch lyrics and multilingual/romanized titles
	if s.lyricsSvc != nil {
		res, err := s.lyricsSvc.FetchLyrics(ctx, title, artist, album)
		if err == nil && res != nil {
			if res.Lyrics != "" && (track.Lyrics == "" || track.LyricsCache == nil || track.LyricsCache["kind"] != "richsync") {
				updateFields["lyrics"] = res.Lyrics
				kind := res.Kind
				if kind == "" {
					kind = "lrc"
				}
				source := res.Source
				if source == "" {
					source = "musixmatch"
				}
				updateFields["lyrics_cache"] = bson.M{
					"kind":       kind,
					"source":     source,
					"text":       res.Lyrics,
					"updated_at": nowTs,
				}
			}
			if len(res.Titles) > 0 {
				updateFields["titles"] = res.Titles
				updateFields["audio.titles"] = res.Titles
			}
		}
	}

	// 7. Ensure titles and audio.titles are populated
	if updateFields["titles"] == nil {
		if len(track.Titles) > 0 {
			cleanOriginal, _ := CleanTitle(fmt.Sprint(track.Titles["original"]), artist)
			if cleanOriginal != "" {
				track.Titles["original"] = cleanOriginal
			}
			updateFields["titles"] = track.Titles
			updateFields["audio.titles"] = track.Titles
		} else {
			updateFields["titles"] = bson.M{"original": title}
			updateFields["audio.titles"] = bson.M{"original": title}
		}
	} else if updateFields["audio.titles"] == nil {
		updateFields["audio.titles"] = updateFields["titles"]
	}

	// 8. Clean unsetting of legacy / erroneous fields inside audio subdocument (preserving audio.titles!)
	unsetFields := bson.M{
		"audio.lyrics":          "",
		"audio.cover_url":       "",
		"audio.file_size":       "",
		"audio.mime_type":       "",
		"enriching":             "",
		"enrichment_error":      "",
		"enrichment_started_at": "",
	}

	// 9. Update track document cleanly
	_, err = s.tracksCol.UpdateOne(ctx, bson.M{"_id": trackID}, bson.M{
		"$set":   updateFields,
		"$unset": unsetFields,
	})
	if err != nil {
		logEnrich.Errorf("Failed to update track %s in mongo: %v", trackID, err)
		return
	}

	// 10. Update artist entity incrementally
	if artist != "" && s.artistsCol != nil {
		slug := strings.ToLower(NormalizeText(artist))
		slug = strings.ReplaceAll(slug, " ", "_")
		artistID := "artist_" + slug
		artistCover, _ := s.coverSearch.FindArtistAvatar(ctx, artist)
		artistUpdate := bson.M{
			"$setOnInsert": bson.M{
				"_id":          artistID,
				"name":         artist,
				"match_artist": strings.ToLower(artist),
				"created_at":   nowTs,
				"followers":    0,
			},
			"$set": bson.M{
				"updated_at": nowTs,
			},
			"$inc": bson.M{
				"tracks_count": 1,
			},
		}
		if artistCover != "" {
			artistUpdate["$set"].(bson.M)["cover_url"] = artistCover
		}
		_, _ = s.artistsCol.UpdateOne(ctx, bson.M{"_id": artistID}, artistUpdate, options.UpdateOne().SetUpsert(true))
	}

	// 11. Update album entity incrementally
	if album != "" && s.albumsCol != nil {
		albumID := GenerateAlbumID(album, year)
		bestCover := track.EffectiveCoverURL()
		if c, ok := updateFields["spotify.cover_url"].(string); ok && c != "" {
			bestCover = c
		}
		albumUpdate := bson.M{
			"$setOnInsert": bson.M{
				"_id":          albumID,
				"title":        album,
				"artist":       artist,
				"artists":      []string{artist},
				"match_album":  strings.ToLower(album),
				"match_artist": strings.ToLower(artist),
				"created_at":   nowTs,
			},
			"$set": bson.M{
				"updated_at": nowTs,
			},
			"$inc": bson.M{
				"tracks_count":   1,
				"duration_total": durationSec,
			},
		}
		if year != nil {
			albumUpdate["$set"].(bson.M)["year"] = *year
		}
		if bestCover != "" {
			albumUpdate["$set"].(bson.M)["cover_url"] = bestCover
		}
		_, _ = s.albumsCol.UpdateOne(ctx, bson.M{"_id": albumID}, albumUpdate, options.UpdateOne().SetUpsert(true))
	}

	logEnrich.Infof("Enriched track %s (%s - %s)", trackID, artist, title)
}

// Stop gracefully waits for in-flight enrichment workers to exit upon context cancellation.
func (s *EnrichmentService) Stop() {
	s.wg.Wait()
}
