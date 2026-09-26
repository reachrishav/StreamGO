package telegram

import (
	"context"
	"fmt"
	"math"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/gotd/td/tg"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"streamgo/internal/config"
	"streamgo/internal/database"
	"streamgo/internal/metadata"
)

// AccessFilter checks chat access rights.
type AccessFilter interface {
	IsChatAllowed(ctx context.Context, chatID int64) bool
}

// DedupChecker checks whether a track has already been indexed.
type DedupChecker interface {
	IsDuplicate(ctx context.Context, fileUniqueID, title, artist, album string, durationSec float64) bool
}

// IngestionListener listens for incoming Telegram audio uploads and indexes them.
type IngestionListener struct {
	cfg          *config.Config
	tracksCol    *mongo.Collection
	accessFilter AccessFilter
	dedupChecker DedupChecker
	onNewTrack   func(trackID string)
	tgService    *Service
}

// NewIngestionListener creates a new IngestionListener.
func NewIngestionListener(
	cfg *config.Config,
	db *database.Client,
	filter AccessFilter,
	dedup DedupChecker,
	onNewTrack func(trackID string),
) *IngestionListener {
	var tracksCol *mongo.Collection
	if db != nil {
		tracksCol = db.Collection("audioTracks")
	}
	return &IngestionListener{
		cfg:          cfg,
		tracksCol:    tracksCol,
		accessFilter: filter,
		dedupChecker: dedup,
		onNewTrack:   onNewTrack,
	}
}

// SetTelegramService attaches the telegram multi-client service to the listener.
func (l *IngestionListener) SetTelegramService(svc *Service) {
	l.tgService = svc
}

// SetupDispatcher configures an update dispatcher to intercept audio uploads.
func (l *IngestionListener) SetupDispatcher(d *tg.UpdateDispatcher) {
	d.OnNewChannelMessage(func(ctx context.Context, e tg.Entities, u *tg.UpdateNewChannelMessage) error {
		if msg, ok := u.Message.(*tg.Message); ok {
			l.handleMessage(ctx, msg)
		}
		return nil
	})

	d.OnNewMessage(func(ctx context.Context, e tg.Entities, u *tg.UpdateNewMessage) error {
		if msg, ok := u.Message.(*tg.Message); ok {
			l.handleMessage(ctx, msg)
		}
		return nil
	})
}

func (l *IngestionListener) handleMessage(ctx context.Context, msg *tg.Message) {
	if msg == nil || msg.Media == nil {
		return
	}

	mediaDoc, ok := msg.Media.(*tg.MessageMediaDocument)
	if !ok || mediaDoc.Document == nil {
		return
	}

	doc, ok := mediaDoc.Document.(*tg.Document)
	if !ok {
		return
	}

	mimeType := strings.ToLower(doc.MimeType)
	var audioAttr *tg.DocumentAttributeAudio
	var fileName string

	for _, attr := range doc.Attributes {
		switch a := attr.(type) {
		case *tg.DocumentAttributeAudio:
			audioAttr = a
		case *tg.DocumentAttributeFilename:
			fileName = a.FileName
		}
	}

	isAudio := strings.HasPrefix(mimeType, "audio/") || audioAttr != nil
	if !isAudio {
		return
	}

	// Extract Chat ID
	var chatID int64
	switch p := msg.PeerID.(type) {
	case *tg.PeerChannel:
		chatID = -1000000000000 - p.ChannelID
	case *tg.PeerChat:
		chatID = -p.ChatID
	case *tg.PeerUser:
		chatID = p.UserID
	}

	// Access filter check
	if l.accessFilter != nil && !l.accessFilter.IsChatAllowed(ctx, chatID) {
		log.Debugf("Ignored audio from unauthorized chat: %d", chatID)
		return
	}

	// Metadata extraction
	title := ""
	artist := ""
	duration := 0.0

	if audioAttr != nil {
		title = strings.TrimSpace(audioAttr.Title)
		artist = strings.TrimSpace(audioAttr.Performer)
		duration = float64(audioAttr.Duration)
	}

	if title == "" {
		title = fileName
	}
	if title == "" {
		title = fmt.Sprintf("Audio %d", msg.ID)
	}

	title, artist = metadata.CleanMetadata(title, artist)

	// Forum topic extraction
	var topicID int32
	topicName := ""
	if msg.ReplyTo != nil {
		if header, ok := msg.ReplyTo.(*tg.MessageReplyHeader); ok {
			if header.ReplyToTopID != 0 {
				topicID = int32(header.ReplyToTopID)
			} else if header.ReplyToMsgID != 0 {
				topicID = int32(header.ReplyToMsgID)
			}
		}
	}
	if topicID != 0 {
		topicName = fmt.Sprintf("topic_%d", topicID)
		if l.tracksCol != nil && l.tracksCol.Database() != nil {
			ftCol := l.tracksCol.Database().Collection("forum_topics")
			var ftDoc struct {
				TopicName string `bson:"topic_name"`
			}
			err := ftCol.FindOne(ctx, bson.M{
				"$or": []bson.M{
					{"_id": fmt.Sprintf("%d:%d", chatID, topicID)},
					{"topic_id": topicID},
				},
				"topic_name": bson.M{"$exists": true, "$ne": ""},
			}).Decode(&ftDoc)
			if err == nil && strings.TrimSpace(ftDoc.TopicName) != "" && !strings.HasPrefix(ftDoc.TopicName, "topic_") {
				topicName = strings.TrimSpace(ftDoc.TopicName)
			}
		}
	}

	// Filter by CHAT_TOPIC if configured
	if l.cfg != nil && l.cfg.ChatTopic != "" && l.cfg.ChatTopic != "all" {
		if l.cfg.ChatTopic == "0" {
			if topicID != 0 {
				return
			}
		} else if targetID, err := strconv.ParseInt(l.cfg.ChatTopic, 10, 64); err == nil {
			if int64(topicID) != targetID {
				return
			}
		}
	}

	// Source and Cache Message / Chat IDs (handle forwards properly)
	cacheChatID := chatID
	cacheMsgID := int32(msg.ID)

	sourceChatID := chatID
	sourceMsgID := int32(msg.ID)

	if fwd, ok := msg.GetFwdFrom(); ok {
		if fromID, ok := fwd.GetFromID(); ok {
			switch p := fromID.(type) {
			case *tg.PeerChannel:
				sourceChatID = -1000000000000 - p.ChannelID
			case *tg.PeerChat:
				sourceChatID = -p.ChatID
			case *tg.PeerUser:
				sourceChatID = p.UserID
			}
		}
		if post, ok := fwd.GetChannelPost(); ok && post != 0 {
			sourceMsgID = int32(post)
		} else if savedMsg, ok := fwd.GetSavedFromMsgID(); ok && savedMsg != 0 {
			sourceMsgID = int32(savedMsg)
		}
	}

	// Telegram File IDs: file_unique_id is the document _id
	fileUniqueID := EncodeFileUniqueID(doc.ID)
	primaryFileID := EncodeFileID(doc.ID, doc.AccessHash, int32(doc.DCID), doc.FileReference)
	trackID := fileUniqueID

	// Extract audio format
	audioExt := "mp3"
	lowerName := strings.ToLower(fileName)
	if strings.Contains(lowerName, "alac") || strings.Contains(mimeType, "alac") {
		audioExt = "alac"
	} else if strings.HasSuffix(lowerName, ".flac") || strings.Contains(mimeType, "flac") {
		audioExt = "flac"
	} else if strings.HasSuffix(lowerName, ".mp3") || strings.Contains(mimeType, "mp3") {
		audioExt = "mp3"
	} else if strings.HasSuffix(lowerName, ".m4a") || strings.Contains(mimeType, "m4a") {
		audioExt = "m4a"
	} else if strings.HasSuffix(lowerName, ".ogg") || strings.Contains(mimeType, "ogg") {
		audioExt = "ogg"
	} else if strings.HasSuffix(lowerName, ".wav") || strings.Contains(mimeType, "wav") {
		audioExt = "wav"
	}

	album := ""
	durSec := int32(math.Round(duration))

	// Deduplication check
	if l.dedupChecker != nil && l.dedupChecker.IsDuplicate(ctx, fileUniqueID, title, artist, album, duration) {
		log.Infof("Skipping duplicate audio track: %s - %s", artist, title)
		return
	}

	// Normalized fingerprint matching Python StreamXBot: title|artist|album|dur
	fingerprint := buildFingerprint(title, artist, album, durSec)
	now := float64(time.Now().Unix())

	// Primary Bot ID for telegram.file_ids map
	botIDKey := ""
	if l.cfg != nil && l.cfg.BotToken != "" {
		parts := strings.Split(l.cfg.BotToken, ":")
		botIDKey = parts[0]
	}
	fileIDs := bson.M{}
	if botIDKey != "" {
		fileIDs[botIDKey] = primaryFileID
	}
	if l.tgService != nil {
		syncCtx, cancel := context.WithTimeout(ctx, 4*time.Second)
		synced := l.tgService.SyncFileIDs(syncCtx, sourceChatID, sourceMsgID, cacheChatID, cacheMsgID)
		cancel()
		for k, v := range synced {
			if v != "" {
				fileIDs[k] = v
			}
		}
	}

	var artists []string
	if artist != "" {
		artists = []string{artist}
	} else {
		artists = []string{}
	}

	normalizedMime := normalizeMimeType(doc.MimeType, fileName)

	// Build Audio object strictly matching original source schema (no titles/file_size/mime/cover/lyrics in audio)
	audioDoc := bson.M{
		"title":        title,
		"artist":       artist,
		"artists":      artists,
		"duration_sec": durSec,
		"type":         audioExt,
	}
	if album != "" {
		audioDoc["album"] = album
		aid := slugify(album)
		if aid != "" {
			audioDoc["album_id"] = "album_" + aid
		}
	}

	setFields := bson.M{
		"source_chat_id":    sourceChatID,
		"source_message_id": sourceMsgID,
		"cache_chat_id":     cacheChatID,
		"cache_message_id":  cacheMsgID,
		"topic_id":          topicID,
		"topic_name":        topicName,
		"fingerprint":        fingerprint,
		"audio":             audioDoc,
		"telegram": bson.M{
			"file_id":        primaryFileID,
			"file_unique_id": fileUniqueID,
			"file_size":      doc.Size,
			"mime_type":      normalizedMime,
			"file_name":      fileName,
			"file_ids":       fileIDs,
		},
		"titles": bson.M{
			"original": title,
		},
		"enriched":   false,
		"indexed":    true,
		"deleted":    false,
		"updated_at": now,
	}

	if l.tracksCol != nil {
		opts := options.UpdateOne().SetUpsert(true)
		_, err := l.tracksCol.UpdateOne(ctx, bson.M{"_id": trackID}, bson.M{
			"$set": setFields,
			"$setOnInsert": bson.M{
				"_id":        trackID,
				"created_at": now,
			},
		}, opts)
		if err == nil {
			log.Infof("Indexed new Telegram audio track: %s (%s - %s)", trackID, artist, title)
			if l.onNewTrack != nil {
				l.onNewTrack(trackID)
			}
		} else {
			log.Errorf("Failed to index audio track %s: %v", trackID, err)
		}
	}
}

func buildFingerprint(title, artist, album string, durSec int32) string {
	t := normalizeListenerText(title)
	a := normalizeListenerText(artist)
	al := normalizeListenerText(album)
	durKey := ""
	if durSec > 0 {
		durKey = fmt.Sprintf("%d", int64(math.Round(float64(durSec)/2.0)*2))
	}
	return strings.Trim(strings.Join([]string{t, a, al, durKey}, "|"), "|")
}

func normalizeListenerText(val string) string {
	s := strings.ToLower(val)
	reNonAlpha := regexp.MustCompile(`[^\p{L}\p{N}]+`)
	s = reNonAlpha.ReplaceAllString(s, " ")
	return strings.TrimSpace(s)
}

func slugify(val string) string {
	s := normalizeListenerText(val)
	return strings.ReplaceAll(s, " ", "_")
}

func normalizeMimeType(mime string, fileName string) string {
	if fileName != "" {
		fn := strings.ToLower(strings.TrimSpace(fileName))
		switch {
		case strings.HasSuffix(fn, ".wav") || strings.HasSuffix(fn, ".wave"):
			return "audio/wav"
		case strings.HasSuffix(fn, ".flac"):
			return "audio/flac"
		case strings.HasSuffix(fn, ".mp3"):
			return "audio/mpeg"
		case strings.HasSuffix(fn, ".m4a"):
			return "audio/mp4"
		case strings.HasSuffix(fn, ".ogg") || strings.HasSuffix(fn, ".opus"):
			return "audio/ogg"
		case strings.HasSuffix(fn, ".aac"):
			return "audio/aac"
		}
	}

	if mime == "" {
		return "audio/mpeg"
	}

	raw := strings.ToLower(strings.TrimSpace(strings.Split(mime, ";")[0]))
	switch {
	case raw == "audio/flac" || raw == "audio/x-flac" || strings.HasSuffix(raw, "/x-flac"):
		return "audio/flac"
	case raw == "audio/wav" || raw == "audio/x-wav" || raw == "audio/wave":
		return "audio/wav"
	case raw == "audio/mp3" || raw == "audio/mpeg":
		return "audio/mpeg"
	case raw == "audio/m4a" || raw == "audio/x-m4a" || raw == "audio/mp4":
		return "audio/mp4"
	case raw == "audio/ogg" || raw == "application/ogg":
		return "audio/ogg"
	case raw == "audio/aac":
		return "audio/aac"
	default:
		return raw
	}
}
