package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"streamgo/internal/config"
	"streamgo/internal/database"
	"streamgo/internal/logger"
	"streamgo/internal/server"
	"streamgo/internal/services"
	"streamgo/internal/telegram"

	"go.mongodb.org/mongo-driver/v2/bson"
)

var log = logger.New("main")

func main() {
	fmt.Println("==================================================")
	fmt.Println("        StreamGO - High Performance Media API     ")
	fmt.Println("==================================================")

	cfg := config.Load()
	log.Infof("loaded config: PORT=%s, DB=%s, API_LOGS=%v", cfg.Port, cfg.DatabaseName, cfg.APILogs)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var dbClient *database.Client
	if cfg.MongoURI != "" {
		dbCtx, dbCancel := context.WithTimeout(ctx, 10*time.Second)
		client, err := database.Connect(dbCtx, cfg.MongoURI, cfg.DatabaseName)
		dbCancel()
		if err != nil {
			log.Warnf("MongoDB connection failed: %v", err)
			log.Warn("Continuing with MongoDB in disabled state...")
		} else {
			dbClient = client
			defer func() {
				closeCtx, closeCancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer closeCancel()
				_ = dbClient.Close(closeCtx)
			}()
		}
	}

	coverSearch := services.NewCoverSearchService()
	lyricsSvc := services.NewLyricsEnrichmentService(cfg)
	dedupSvc := services.NewDedupService(dbClient)
	accessFilter := services.NewAccessFilter(cfg, dbClient)
	r2Storage := services.NewR2StorageService(cfg)
	enrichSvc := services.NewEnrichmentService(dbClient, coverSearch, lyricsSvc)
	if r2Storage.IsConfigured() {
		enrichSvc.SetR2Storage(r2Storage)
		log.Infof("Cloudflare R2 storage configured for artwork (bucket: %s)", cfg.R2BucketName)
	} else {
		log.Info("Cloudflare R2 not configured; using free online artwork enrichment (iTunes/Deezer)")
	}

	if dbClient != nil {
		workers := cfg.EnrichmentWorkers
		if workers <= 0 {
			workers = 2
		}
		enrichSvc.Start(ctx, workers)

		// Fix any existing tracks where audio format was saved as "m4a" but has lossless bit depth (ALAC)
		go func() {
			migCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			col := dbClient.Database.Collection("audioTracks")
			res, err := col.UpdateMany(migCtx, bson.M{
				"audio.type":      "m4a",
				"audio.bit_depth": bson.M{"$gt": 0},
			}, bson.M{
				"$set": bson.M{"audio.type": "alac"},
			})
			if err == nil && res.ModifiedCount > 0 {
				log.Infof("[migration] Normalized %d lossless ALAC track(s) from 'm4a' to 'alac'", res.ModifiedCount)
			}
		}()
	}

	var tgService *telegram.Service
	if cfg.ApiID > 0 && cfg.ApiHash != "" {
		svc, err := telegram.New(cfg)
		if err != nil {
			log.Warnf("Failed to initialize Telegram service: %v", err)
		} else {
			tgService = svc
			enrichSvc.SetDownloader(tgService)
			listener := telegram.NewIngestionListener(cfg, dbClient, accessFilter, dedupSvc, enrichSvc.TriggerEnrich)
			listener.SetTelegramService(tgService)
			listener.SetupDispatcher(&tgService.Dispatcher)

			log.Info("Starting Telegram MTProto multi-client pool in background...")
			go func() {
				if err := tgService.Start(ctx); err != nil {
					log.Errorf("Telegram client start failed: %v", err)
				}
			}()
			defer tgService.Stop()
		}
	} else {
		log.Info("Telegram credentials (API_ID/API_HASH) not configured; skipping Telegram init.")
	}

	srv := server.New(cfg, dbClient, tgService, accessFilter)
	httpServer := &http.Server{
		Addr:         fmt.Sprintf(":%s", cfg.Port),
		Handler:      srv.Router,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 0,
		IdleTimeout:  120 * time.Second,
	}

	go func() {
		log.Infof("HTTP server listening on http://0.0.0.0:%s", cfg.Port)
		if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("HTTP server error: %v", err)
		}
	}()

	signal.Ignore(syscall.SIGHUP)
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	sig := <-quit

	log.Infof("received shutdown signal (%s), starting graceful shutdown...", sig)
	cancel() // Concurrently signals background workers (enrichment, telegram) to abort

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()

	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		log.Errorf("HTTP server forced shutdown error: %v", err)
	} else {
		log.Info("HTTP server shutdown cleanly.")
	}

	enrichSvc.Stop()
	log.Info("StreamGO stopped. Goodbye!")
}
