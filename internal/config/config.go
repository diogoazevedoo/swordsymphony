package config

import (
	"os"
	"strconv"
	"time"
)

// Config holds all application configuration
type Config struct {
	Server     ServerConfig
	Database   DatabaseConfig
	AI         AIConfig
	Medical    MedicalConfig
	Twilio     TwilioConfig
	ElevenLabs ElevenLabsConfig
	Deepgram   DeepgramConfig
	Email      EmailConfig
}

// ServerConfig holds server-related configuration
type ServerConfig struct {
	Port         string
	ReadTimeout  time.Duration
	WriteTimeout time.Duration
}

// DatabaseConfig holds database-related configuration
type DatabaseConfig struct {
	Driver   string
	Host     string
	Port     int
	User     string
	Password string
	Database string
}

// AIConfig holds AI-related configuration
type AIConfig struct {
	Provider    string
	APIKey      string
	ModelName   string
	MaxTokens   int
	Temperature float64
}

// MedicalConfig holds medical knowledge base configuration
type MedicalConfig struct {
	DataPath string
}

// TwilioConfig holds Twilio configuration
type TwilioConfig struct {
	AccountSID     string
	AuthToken      string
	PhoneNumber    string
	WebhookBaseURL string
}

// ElevenLabsConfig holds ElevenLabs configuration
type ElevenLabsConfig struct {
	APIKey          string
	VoiceID         string
	ModelID         string
	Stability       float64
	SimilarityBoost float64
}

// DeepgramConfig holds Deepgram configuration
type DeepgramConfig struct {
	APIKey   string
	Language string
	Model    string
	Tier     string
}

// EmailConfig holds email configuration
type EmailConfig struct {
	SMTPServer   string
	SMTPPort     int
	Username     string
	Password     string
	FromAddress  string
	FromName     string
	TemplatesDir string
}

// LoadConfig loads configuration from environment variables
func LoadConfig() (*Config, error) {
	port := getEnv("PORT", "8080")
	readTimeoutStr := getEnv("SERVER_READ_TIMEOUT", "5")
	writeTimeoutStr := getEnv("SERVER_WRITE_TIMEOUT", "10")

	readTimeout, err := strconv.Atoi(readTimeoutStr)
	if err != nil {
		readTimeout = 5
	}

	writeTimeout, err := strconv.Atoi(writeTimeoutStr)
	if err != nil {
		writeTimeout = 10
	}

	dbDriver := getEnv("DB_DRIVER", "postgres")
	dbHost := getEnv("DB_HOST", "localhost")
	dbPortStr := getEnv("DB_PORT", "5432")
	dbPort, err := strconv.Atoi(dbPortStr)
	if err != nil {
		dbPort = 5432
	}
	dbUser := getEnv("DB_USER", "postgres")
	dbPassword := getEnv("DB_PASSWORD", "")
	dbName := getEnv("DB_NAME", "swordsymphony")

	aiProvider := getEnv("AI_PROVIDER", "openai")
	aiAPIKey := getEnv("OPENAI_API_KEY", "")
	aiModelName := getEnv("AI_MODEL_NAME", "gpt-4")
	aiMaxTokensStr := getEnv("AI_MAX_TOKENS", "1024")
	aiTemperatureStr := getEnv("AI_TEMPERATURE", "0.3")

	aiMaxTokens, err := strconv.Atoi(aiMaxTokensStr)
	if err != nil {
		aiMaxTokens = 1024
	}

	aiTemperature, err := strconv.ParseFloat(aiTemperatureStr, 64)
	if err != nil {
		aiTemperature = 0.3
	}

	medicalDataPath := getEnv("MEDICAL_DATA_PATH", "")

	// Twilio configuration
	twilioAccountSID := getEnv("TWILIO_ACCOUNT_SID", "")
	twilioAuthToken := getEnv("TWILIO_AUTH_TOKEN", "")
	twilioPhoneNumber := getEnv("TWILIO_PHONE_NUMBER", "")
	twilioWebhookBaseURL := getEnv("TWILIO_WEBHOOK_BASE_URL", "")

	// ElevenLabs configuration
	elevenLabsAPIKey := getEnv("ELEVENLABS_API_KEY", "")
	elevenLabsVoiceID := getEnv("ELEVENLABS_VOICE_ID", "EXAVITQu4vr4xnSDxMaL")
	elevenLabsModelID := getEnv("ELEVENLABS_MODEL_ID", "eleven_monolingual_v1")
	elevenLabsStabilityStr := getEnv("ELEVENLABS_STABILITY", "0.5")
	elevenLabsSimilarityBoostStr := getEnv("ELEVENLABS_SIMILARITY_BOOST", "0.75")

	elevenLabsStability, err := strconv.ParseFloat(elevenLabsStabilityStr, 64)
	if err != nil {
		elevenLabsStability = 0.5
	}

	elevenLabsSimilarityBoost, err := strconv.ParseFloat(elevenLabsSimilarityBoostStr, 64)
	if err != nil {
		elevenLabsSimilarityBoost = 0.75
	}

	// Deepgram configuration
	deepgramAPIKey := getEnv("DEEPGRAM_API_KEY", "")
	deepgramLanguage := getEnv("DEEPGRAM_LANGUAGE", "en-US")
	deepgramModel := getEnv("DEEPGRAM_MODEL", "general")
	deepgramTier := getEnv("DEEPGRAM_TIER", "enhanced")

	// Email configuration
	emailSMTPServer := getEnv("EMAIL_SMTP_SERVER", "smtp.gmail.com")
	emailSMTPPortStr := getEnv("EMAIL_SMTP_PORT", "587")
	emailUsername := getEnv("EMAIL_USERNAME", "")
	emailPassword := getEnv("EMAIL_PASSWORD", "")
	emailFromAddress := getEnv("EMAIL_FROM_ADDRESS", emailUsername)
	emailFromName := getEnv("EMAIL_FROM_NAME", "SwordSymphony Medical AI")
	emailTemplatesDir := getEnv("EMAIL_TEMPLATES_DIR", "./templates/email")

	emailSMTPPort, err := strconv.Atoi(emailSMTPPortStr)
	if err != nil {
		emailSMTPPort = 587
	}

	return &Config{
		Server: ServerConfig{
			Port:         port,
			ReadTimeout:  time.Duration(readTimeout) * time.Second,
			WriteTimeout: time.Duration(writeTimeout) * time.Second,
		},
		Database: DatabaseConfig{
			Driver:   dbDriver,
			Host:     dbHost,
			Port:     dbPort,
			User:     dbUser,
			Password: dbPassword,
			Database: dbName,
		},
		AI: AIConfig{
			Provider:    aiProvider,
			APIKey:      aiAPIKey,
			ModelName:   aiModelName,
			MaxTokens:   aiMaxTokens,
			Temperature: aiTemperature,
		},
		Medical: MedicalConfig{
			DataPath: medicalDataPath,
		},
		Twilio: TwilioConfig{
			AccountSID:     twilioAccountSID,
			AuthToken:      twilioAuthToken,
			PhoneNumber:    twilioPhoneNumber,
			WebhookBaseURL: twilioWebhookBaseURL,
		},
		ElevenLabs: ElevenLabsConfig{
			APIKey:          elevenLabsAPIKey,
			VoiceID:         elevenLabsVoiceID,
			ModelID:         elevenLabsModelID,
			Stability:       elevenLabsStability,
			SimilarityBoost: elevenLabsSimilarityBoost,
		},
		Deepgram: DeepgramConfig{
			APIKey:   deepgramAPIKey,
			Language: deepgramLanguage,
			Model:    deepgramModel,
			Tier:     deepgramTier,
		},
		Email: EmailConfig{
			SMTPServer:   emailSMTPServer,
			SMTPPort:     emailSMTPPort,
			Username:     emailUsername,
			Password:     emailPassword,
			FromAddress:  emailFromAddress,
			FromName:     emailFromName,
			TemplatesDir: emailTemplatesDir,
		},
	}, nil
}

// Helper to get environment variable with fallback
func getEnv(key, fallback string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	return fallback
}
