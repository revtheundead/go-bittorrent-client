package ui

import (
	"fmt"
	"strings"
	"time"

	"github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"

	"github.com/revtheundead/revtorrent/pkg/bittorrent"
)

const (
	progressBarWidth  = 40
	updateInterval    = 200 * time.Millisecond
)

// Model represents the bubbletea model for download progress
type Model struct {
	torrent       *bittorrent.Torrent
	stats         bittorrent.Stats
	progress      float64
	lastUpdate    time.Time
	startTime     time.Time
	quitting      bool
	err           error
	width         int
	height        int
}

// NewModel creates a new TUI model for a torrent
func NewModel(torrent *bittorrent.Torrent) Model {
	return Model{
		torrent:    torrent,
		startTime:  time.Now(),
		lastUpdate: time.Now(),
	}
}

// Init initializes the model
func (m Model) Init() tea.Cmd {
	return tea.Batch(
		tickCmd(),
		waitForEvents(m.torrent),
	)
}

// Update handles messages and updates the model
func (m Model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.KeyMsg:
		switch msg.String() {
		case "q", "ctrl+c":
			m.quitting = true
			return m, tea.Quit
		}

	case tea.WindowSizeMsg:
		m.width = msg.Width
		m.height = msg.Height

	case tickMsg:
		m.lastUpdate = time.Now()
		m.progress = m.torrent.Progress()
		m.stats = m.torrent.Stats()
		return m, tickCmd()

	case eventMsg:
		switch msg.event.Type {
		case bittorrent.EventComplete:
			// Download complete, continue showing stats
		case bittorrent.EventError:
			m.err = msg.event.Error
			m.quitting = true
			return m, tea.Quit
		}
		return m, waitForEvents(m.torrent)

	case errMsg:
		m.err = msg.err
		m.quitting = true
		return m, tea.Quit
	}

	return m, nil
}

// View renders the UI
func (m Model) View() string {
	if m.quitting {
		if m.err != nil {
			return errorStyle.Render(fmt.Sprintf("Error: %v\n", m.err))
		}
		return successStyle.Render("Download complete!\n")
	}

	var b strings.Builder

	// Title
	b.WriteString(titleStyle.Render(m.torrent.Name()))
	b.WriteString("\n\n")

	// Progress bar
	b.WriteString(m.renderProgressBar())
	b.WriteString("\n\n")

	// Statistics
	b.WriteString(m.renderStats())
	b.WriteString("\n\n")

	// Help text
	b.WriteString(helpStyle.Render("Press q to quit"))

	return b.String()
}

// renderProgressBar renders the progress bar
func (m Model) renderProgressBar() string {
	percent := m.progress * 100
	filledWidth := int(float64(progressBarWidth) * m.progress)

	var bar strings.Builder
	bar.WriteString("[")

	for i := 0; i < progressBarWidth; i++ {
		if i < filledWidth {
			bar.WriteString(progressFilled)
		} else {
			bar.WriteString(progressEmpty)
		}
	}

	bar.WriteString("]")

	progressText := fmt.Sprintf("%.1f%%", percent)
	barStyle := progressBarStyle
	if percent >= 100 {
		barStyle = completedBarStyle
	}

	return fmt.Sprintf("%s %s",
		barStyle.Render(bar.String()),
		percentStyle.Render(progressText))
}

// renderStats renders the download statistics
func (m Model) renderStats() string {
	var b strings.Builder

	// Downloaded / Uploaded
	b.WriteString(labelStyle.Render("Downloaded: "))
	b.WriteString(valueStyle.Render(formatBytes(m.stats.Downloaded)))
	b.WriteString("  ")
	b.WriteString(labelStyle.Render("Uploaded: "))
	b.WriteString(valueStyle.Render(formatBytes(m.stats.Uploaded)))
	b.WriteString("\n")

	// Download / Upload speed
	b.WriteString(labelStyle.Render("Down: "))
	b.WriteString(speedStyle.Render(formatBytes(int64(m.stats.DownloadRate)) + "/s"))
	b.WriteString("  ")
	b.WriteString(labelStyle.Render("Up: "))
	b.WriteString(speedStyle.Render(formatBytes(int64(m.stats.UploadRate)) + "/s"))
	b.WriteString("\n")

	// Peers
	b.WriteString(labelStyle.Render("Peers: "))
	b.WriteString(valueStyle.Render(fmt.Sprintf("%d", m.stats.Peers)))
	b.WriteString("\n")

	// ETA
	if m.stats.DownloadRate > 0 && m.progress < 1.0 {
		remaining := float64(m.torrent.Size()) * (1.0 - m.progress)
		etaSeconds := remaining / m.stats.DownloadRate
		b.WriteString(labelStyle.Render("ETA: "))
		b.WriteString(valueStyle.Render(formatDuration(int64(etaSeconds))))
		b.WriteString("\n")
	}

	// State
	b.WriteString(labelStyle.Render("State: "))
	b.WriteString(stateStyle(m.stats.State).Render(m.stats.State.String()))

	return b.String()
}

// Messages

type tickMsg time.Time

type eventMsg struct {
	event bittorrent.Event
}

type errMsg struct {
	err error
}

// Commands

func tickCmd() tea.Cmd {
	return tea.Tick(updateInterval, func(t time.Time) tea.Msg {
		return tickMsg(t)
	})
}

func waitForEvents(torrent *bittorrent.Torrent) tea.Cmd {
	return func() tea.Msg {
		event := <-torrent.Events()
		return eventMsg{event: event}
	}
}

// Utility functions

func formatBytes(bytes int64) string {
	const unit = 1024
	if bytes < unit {
		return fmt.Sprintf("%d B", bytes)
	}

	div, exp := int64(unit), 0
	for n := bytes / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}

	units := []string{"KB", "MB", "GB", "TB"}
	return fmt.Sprintf("%.1f %s", float64(bytes)/float64(div), units[exp])
}

func formatDuration(seconds int64) string {
	if seconds < 60 {
		return fmt.Sprintf("%ds", seconds)
	}

	minutes := seconds / 60
	if minutes < 60 {
		return fmt.Sprintf("%dm %ds", minutes, seconds%60)
	}

	hours := minutes / 60
	if hours < 24 {
		return fmt.Sprintf("%dh %dm", hours, minutes%60)
	}

	days := hours / 24
	return fmt.Sprintf("%dd %dh", days, hours%24)
}

// Styles

const (
	progressFilled = "█"
	progressEmpty  = "░"
)

var (
	titleStyle = lipgloss.NewStyle().
			Bold(true).
			Foreground(lipgloss.Color("42")).
			MarginBottom(1)

	progressBarStyle = lipgloss.NewStyle().
				Foreground(lipgloss.Color("42"))

	completedBarStyle = lipgloss.NewStyle().
				Foreground(lipgloss.Color("46"))

	percentStyle = lipgloss.NewStyle().
			Bold(true).
			Foreground(lipgloss.Color("42"))

	labelStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("240"))

	valueStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("255"))

	speedStyle = lipgloss.NewStyle().
			Bold(true).
			Foreground(lipgloss.Color("33"))

	helpStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("240")).
			Italic(true)

	errorStyle = lipgloss.NewStyle().
			Bold(true).
			Foreground(lipgloss.Color("196"))

	successStyle = lipgloss.NewStyle().
			Bold(true).
			Foreground(lipgloss.Color("46"))
)

func stateStyle(state bittorrent.TorrentState) lipgloss.Style {
	switch state {
	case bittorrent.StateDownloading:
		return lipgloss.NewStyle().Foreground(lipgloss.Color("33"))
	case bittorrent.StateSeeding:
		return lipgloss.NewStyle().Foreground(lipgloss.Color("46"))
	case bittorrent.StatePaused:
		return lipgloss.NewStyle().Foreground(lipgloss.Color("240"))
	case bittorrent.StateError:
		return lipgloss.NewStyle().Foreground(lipgloss.Color("196"))
	default:
		return lipgloss.NewStyle().Foreground(lipgloss.Color("255"))
	}
}
