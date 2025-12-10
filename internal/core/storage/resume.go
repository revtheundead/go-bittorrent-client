package storage

import (
	"crypto/sha1"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

// ResumeData holds the state needed to resume a download
type ResumeData struct {
	InfoHash        string     `json:"info_hash"`
	Name            string     `json:"name"`
	TotalSize       int64      `json:"total_size"`
	PieceLength     int64      `json:"piece_length"`
	NumPieces       int        `json:"num_pieces"`
	CompletedPieces []int      `json:"completed_pieces"`
	Bitfield        []byte     `json:"bitfield"`
	DownloadedBytes int64      `json:"downloaded_bytes"`
	UploadedBytes   int64      `json:"uploaded_bytes"`
	SavePath        string     `json:"save_path"`
	AddedAt         time.Time  `json:"added_at"`
	UpdatedAt       time.Time  `json:"updated_at"`
	CompletedAt     *time.Time `json:"completed_at,omitempty"`
	IsComplete      bool       `json:"is_complete"`
}

// ResumeManager manages resume data for torrents
type ResumeManager struct {
	stateDir string
}

// NewResumeManager creates a new resume manager
func NewResumeManager(stateDir string) (*ResumeManager, error) {
	// Create state directory if it doesn't exist
	if err := os.MkdirAll(stateDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create state directory: %w", err)
	}

	return &ResumeManager{
		stateDir: stateDir,
	}, nil
}

// Save saves resume data to disk
func (rm *ResumeManager) Save(data *ResumeData) error {
	data.UpdatedAt = time.Now()

	// Generate filename from info hash
	filename := rm.getResumeFilePath(data.InfoHash)

	// Marshal to JSON
	jsonData, err := json.MarshalIndent(data, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal resume data: %w", err)
	}

	// Write to file
	if err := os.WriteFile(filename, jsonData, 0644); err != nil {
		return fmt.Errorf("failed to write resume file: %w", err)
	}

	return nil
}

// Load loads resume data from disk
func (rm *ResumeManager) Load(infoHash string) (*ResumeData, error) {
	filename := rm.getResumeFilePath(infoHash)

	// Check if file exists
	if _, err := os.Stat(filename); os.IsNotExist(err) {
		return nil, fmt.Errorf("no resume data found for %s", infoHash)
	}

	// Read file
	jsonData, err := os.ReadFile(filename)
	if err != nil {
		return nil, fmt.Errorf("failed to read resume file: %w", err)
	}

	// Unmarshal
	var data ResumeData
	if err := json.Unmarshal(jsonData, &data); err != nil {
		return nil, fmt.Errorf("failed to unmarshal resume data: %w", err)
	}

	return &data, nil
}

// Delete removes resume data from disk
func (rm *ResumeManager) Delete(infoHash string) error {
	filename := rm.getResumeFilePath(infoHash)

	if err := os.Remove(filename); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to delete resume file: %w", err)
	}

	return nil
}

// Exists checks if resume data exists for a given info hash
func (rm *ResumeManager) Exists(infoHash string) bool {
	filename := rm.getResumeFilePath(infoHash)
	_, err := os.Stat(filename)
	return err == nil
}

// List returns all resume data files
func (rm *ResumeManager) List() ([]*ResumeData, error) {
	pattern := filepath.Join(rm.stateDir, "*.resume.json")
	files, err := filepath.Glob(pattern)
	if err != nil {
		return nil, fmt.Errorf("failed to list resume files: %w", err)
	}

	result := make([]*ResumeData, 0, len(files))
	for _, file := range files {
		jsonData, err := os.ReadFile(file)
		if err != nil {
			continue // Skip files we can't read
		}

		var data ResumeData
		if err := json.Unmarshal(jsonData, &data); err != nil {
			continue // Skip files we can't parse
		}

		result = append(result, &data)
	}

	return result, nil
}

// getResumeFilePath returns the file path for resume data
func (rm *ResumeManager) getResumeFilePath(infoHash string) string {
	return filepath.Join(rm.stateDir, fmt.Sprintf("%s.resume.json", infoHash))
}

// CreateResumeData creates resume data from storage
func CreateResumeData(infoHash string, name string, storage *FileStorage) *ResumeData {
	completed := storage.bitfield.GetCompletedIndices()
	downloadedBytes := int64(len(completed)) * storage.info.PieceLength

	// Adjust for last piece if it's smaller
	if len(completed) > 0 {
		lastPiece := completed[len(completed)-1]
		if lastPiece == storage.bitfield.Len()-1 {
			// This is the last piece, calculate actual size
			lastPieceSize := storage.info.TotalLength() % storage.info.PieceLength
			if lastPieceSize == 0 {
				lastPieceSize = storage.info.PieceLength
			}
			downloadedBytes = downloadedBytes - storage.info.PieceLength + lastPieceSize
		}
	}

	isComplete := storage.bitfield.IsComplete()
	var completedAt *time.Time
	if isComplete {
		now := time.Now()
		completedAt = &now
	}

	return &ResumeData{
		InfoHash:        infoHash,
		Name:            name,
		TotalSize:       storage.info.TotalLength(),
		PieceLength:     storage.info.PieceLength,
		NumPieces:       storage.bitfield.Len(),
		CompletedPieces: completed,
		Bitfield:        storage.bitfield.GetBytes(),
		DownloadedBytes: downloadedBytes,
		// UploadedBytes: Upload tracking will be implemented in the engine layer
		// which coordinates between storage and uploader components
		UploadedBytes: 0,
		SavePath:      storage.basePath,
		AddedAt:       time.Now(),
		UpdatedAt:     time.Now(),
		CompletedAt:   completedAt,
		IsComplete:    isComplete,
	}
}

// VerifyResumeData verifies the integrity of resume data against storage
func VerifyResumeData(data *ResumeData, storage *FileStorage) error {
	if data.InfoHash == "" {
		return fmt.Errorf("invalid resume data: missing info hash")
	}

	if data.NumPieces != storage.bitfield.Len() {
		return fmt.Errorf("resume data mismatch: expected %d pieces, got %d",
			storage.bitfield.Len(), data.NumPieces)
	}

	if data.TotalSize != storage.info.TotalLength() {
		return fmt.Errorf("resume data mismatch: expected size %d, got %d",
			storage.info.TotalLength(), data.TotalSize)
	}

	return nil
}

// ApplyResumeData applies resume data to storage and verifies pieces
func ApplyResumeData(data *ResumeData, storage *FileStorage) error {
	// Verify resume data matches storage
	if err := VerifyResumeData(data, storage); err != nil {
		return err
	}

	// Apply bitfield
	if len(data.Bitfield) > 0 {
		storage.bitfield = NewBitfield(data.NumPieces)
		storage.bitfield.SetBytes(data.Bitfield)
	} else if len(data.CompletedPieces) > 0 {
		// Fallback to completed pieces list
		for _, index := range data.CompletedPieces {
			storage.bitfield.Set(index)
		}
	}

	// Verify completed pieces by hashing
	verifiedCount := 0
	failedCount := 0

	for _, index := range data.CompletedPieces {
		if index >= storage.bitfield.Len() {
			continue
		}

		valid, err := storage.VerifyPiece(index)
		if err != nil {
			failedCount++
			storage.bitfield.Clear(index)
			continue
		}

		if !valid {
			failedCount++
			storage.bitfield.Clear(index)
		} else {
			verifiedCount++
		}
	}

	if failedCount > 0 {
		return fmt.Errorf("verified %d pieces, %d failed verification",
			verifiedCount, failedCount)
	}

	return nil
}

// CalculateProgress calculates download progress from resume data
func (rd *ResumeData) CalculateProgress() float64 {
	if rd.TotalSize == 0 {
		return 0.0
	}

	return float64(rd.DownloadedBytes) / float64(rd.TotalSize)
}

// PiecesRemaining returns the number of pieces remaining to download
func (rd *ResumeData) PiecesRemaining() int {
	return rd.NumPieces - len(rd.CompletedPieces)
}

// BytesRemaining returns the number of bytes remaining to download
func (rd *ResumeData) BytesRemaining() int64 {
	return rd.TotalSize - rd.DownloadedBytes
}

// CanResume checks if the resume data is valid for resuming
func (rd *ResumeData) CanResume() bool {
	return rd.InfoHash != "" &&
		rd.NumPieces > 0 &&
		rd.TotalSize > 0 &&
		!rd.IsComplete
}

// BackupResumeData creates a backup of resume data
func (rm *ResumeManager) BackupResumeData(infoHash string) error {
	srcPath := rm.getResumeFilePath(infoHash)
	backupPath := srcPath + ".backup"

	data, err := os.ReadFile(srcPath)
	if err != nil {
		return fmt.Errorf("failed to read resume data: %w", err)
	}

	if err := os.WriteFile(backupPath, data, 0644); err != nil {
		return fmt.Errorf("failed to write backup: %w", err)
	}

	return nil
}

// RestoreResumeData restores resume data from backup
func (rm *ResumeManager) RestoreResumeData(infoHash string) error {
	backupPath := rm.getResumeFilePath(infoHash) + ".backup"
	resumePath := rm.getResumeFilePath(infoHash)

	data, err := os.ReadFile(backupPath)
	if err != nil {
		return fmt.Errorf("failed to read backup: %w", err)
	}

	if err := os.WriteFile(resumePath, data, 0644); err != nil {
		return fmt.Errorf("failed to restore resume data: %w", err)
	}

	return nil
}

// InfoHashFromBytes calculates SHA1 hash for info hash representation
func InfoHashFromBytes(data []byte) string {
	hash := sha1.Sum(data)
	return fmt.Sprintf("%x", hash)
}
