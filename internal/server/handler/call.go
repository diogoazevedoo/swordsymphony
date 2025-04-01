package handler

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"

	"github.com/diogoazevedoo/swordsymphony/internal/communication/call"
	"github.com/diogoazevedoo/swordsymphony/internal/logger"
	"github.com/gin-gonic/gin"
)

// CallController handles phone call-related API endpoints
type CallController struct {
	callService *call.Service
	audioFiles  map[string][]byte
	audioMutex  sync.RWMutex
}

// NewCallController creates a new call controller
func NewCallController(callService *call.Service) *CallController {
	return &CallController{
		callService: callService,
		audioFiles:  make(map[string][]byte),
	}
}

// StartCall initiates a phone call to a patient
func (c *CallController) StartCall(ctx *gin.Context) {
	var request struct {
		PhoneNumber string `json:"phone_number" binding:"required"`
	}

	if err := ctx.ShouldBindJSON(&request); err != nil {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request", "details": err.Error()})
		return
	}

	callSID, err := c.callService.InitiateCall(request.PhoneNumber)
	if err != nil {
		ctx.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to start call", "details": err.Error()})
		return
	}

	ctx.JSON(http.StatusOK, gin.H{
		"status":   "success",
		"message":  "Call initiated successfully",
		"call_sid": callSID,
	})
}

// EndCall terminates an active call
func (c *CallController) EndCall(ctx *gin.Context) {
	callSID := ctx.Param("call_sid")
	if callSID == "" {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": "Call SID is required"})
		return
	}

	if err := c.callService.EndCall(callSID); err != nil {
		ctx.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to end call", "details": err.Error()})
		return
	}

	ctx.JSON(http.StatusOK, gin.H{
		"status":  "success",
		"message": "Call ended successfully",
	})
}

// GetCallResults retrieves the results of a call
func (c *CallController) GetCallResults(ctx *gin.Context) {
	callSID := ctx.Param("call_sid")
	if callSID == "" {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": "Call SID is required"})
		return
	}

	results, err := c.callService.ProcessCallResults(callSID)
	if err != nil {
		ctx.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to get call results", "details": err.Error()})
		return
	}

	ctx.JSON(http.StatusOK, gin.H{
		"status":  "success",
		"results": results,
	})
}

// HandleWebhook processes Twilio webhook requests
func (c *CallController) HandleWebhook(ctx *gin.Context) {
	webhookHandler := c.callService.GetWebhookHandler()
	webhookHandler.HandleIncomingCall(ctx)
}

// HandleStatusCallback processes Twilio status webhook requests
func (c *CallController) HandleStatusCallback(ctx *gin.Context) {
	webhookHandler := c.callService.GetWebhookHandler()
	webhookHandler.HandleStatusCallback(ctx)
}

// HandleSpeechCallback processes speech input from Twilio with improved content awareness
func (c *CallController) HandleSpeechCallback(ctx *gin.Context) {
	callSID := ctx.Query("call_sid")
	if callSID == "" {
		logger.Error("Speech callback missing call_sid")
		ctx.JSON(http.StatusBadRequest, gin.H{"error": "Call SID is required"})
		return
	}

	speechInput := ctx.PostForm("SpeechResult")
	logger.Info("Speech callback received",
		"call_sid", callSID,
		"input", speechInput,
		"input_length", len(speechInput))

	// If no speech was detected, provide a simple response
	if speechInput == "" {
		logger.Warn("Empty speech input received", "call_sid", callSID)
		c.respondWithSimpleTwiML(ctx, callSID, "I didn't catch that. Let me ask you a different question.")
		return
	}

	// Simply store the message - don't wait for processing
	err := c.callService.StoreMessage(callSID, "patient", speechInput)
	if err != nil {
		logger.Error("Failed to store patient message", "error", err)
	}

	// Generate a response URL that will stream the real response when ready
	responseURL := fmt.Sprintf("%s/api/call/response/%s", c.callService.GetBaseURL(), callSID)

	// Create TwiML response that will play our audio and gather the next speech input
	twiML := `<?xml version="1.0" encoding="UTF-8"?><Response>
        <Play>` + responseURL + `</Play>
        <Gather input="speech" action="/api/call/speech?call_sid=` + callSID + `" language="en-US" speechTimeout="auto"/>
    </Response>`

	ctx.Header("Content-Type", "text/xml")
	ctx.String(http.StatusOK, twiML)

	// Process the speech input and prepare response asynchronously
	go func() {
		// Generate the next response in the sequence
		aiResponse, err := c.callService.HandleSpeechInput(callSID, speechInput)
		if err != nil || aiResponse == "" {
			logger.Warn("Failed to generate AI response, using fallback",
				"call_sid", callSID,
				"error", err)
			aiResponse = "Let me ask you the next question."
		}

		// Generate audio from the response
		_, audioErr := c.callService.GenerateAndStoreAudioResponse(callSID, aiResponse)
		if audioErr != nil {
			logger.Error("Error generating audio, will fall back to TTS",
				"error", audioErr,
				"call_sid", callSID)
		}

		// Force check for conversation completion to end call if necessary
		c.callService.CheckConversationCompletion(callSID)
	}()
}

// processAndPrepareResponse handles the AI response generation and audio conversion
func (c *CallController) processAndPrepareResponse(callSID string, speechInput string) {
	// Set a reasonable timeout for the entire processing pipeline
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()

	// Try multiple times with shorter timeouts
	var aiResponse string
	var err error
	maxRetries := 3

	for attempt := 0; attempt < maxRetries; attempt++ {
		// Create a shorter timeout for each attempt
		attemptCtx, attemptCancel := context.WithTimeout(ctx, 5*time.Second)

		// Use a channel to handle the timeout
		responseCh := make(chan string, 1)
		errCh := make(chan error, 1)

		go func() {
			resp, e := c.callService.HandleSpeechInput(callSID, speechInput)
			if e != nil {
				errCh <- e
				return
			}
			responseCh <- resp
		}()

		// Wait for response or timeout
		select {
		case aiResponse = <-responseCh:
			attemptCancel()
			err = nil
			break
		case err = <-errCh:
			attemptCancel()
			logger.Warn("Retry handling speech input",
				"call_sid", callSID,
				"attempt", attempt+1,
				"error", err)
			time.Sleep(500 * time.Millisecond)
			continue
		case <-attemptCtx.Done():
			attemptCancel()
			logger.Warn("Timeout getting AI response",
				"call_sid", callSID,
				"attempt", attempt+1)
			time.Sleep(500 * time.Millisecond)
			continue
		}
	}

	if err != nil || aiResponse == "" {
		logger.Error("Failed to generate AI response after retries",
			"call_sid", callSID,
			"error", err)
		aiResponse = "I'm having trouble connecting right now. Let me try again. Could you repeat what you were saying?"
	}

	// Generate audio with a shorter timeout
	audioCtx, audioCancel := context.WithTimeout(ctx, 10*time.Second)
	defer audioCancel()

	audioCh := make(chan []byte, 1)
	audioErrCh := make(chan error, 1)

	go func() {
		audioBytes, e := c.callService.GenerateAndStoreAudioResponse(callSID, aiResponse)
		if e != nil {
			audioErrCh <- e
			return
		}
		audioCh <- audioBytes
	}()

	select {
	case <-audioCh:
		logger.Info("Successfully generated and stored audio response", "call_sid", callSID)
	case err := <-audioErrCh:
		logger.Error("Failed to generate audio", "error", err, "call_sid", callSID)
		// The fallback audio will be handled by the response endpoint
	case <-audioCtx.Done():
		logger.Error("Timeout generating audio", "call_sid", callSID)
		// The fallback audio will be handled by the response endpoint
	}
}

// respondWithSimpleTwiML is a helper to quickly respond with TwiML
func (c *CallController) respondWithSimpleTwiML(ctx *gin.Context, callSID, message string) {
	twiML := `<?xml version="1.0" encoding="UTF-8"?><Response>
		<Say voice="alice">` + message + `</Say>
		<Gather input="speech" action="/api/call/speech?call_sid=` + callSID + `" language="en-US" speechTimeout="auto"/>
	</Response>`

	ctx.Header("Content-Type", "text/xml")
	ctx.String(http.StatusOK, twiML)
}

func (c *CallController) StoreAudio(ctx *gin.Context) {
	callSID := ctx.Param("call_sid")
	if callSID == "" {
		logger.Error("Store audio request missing call_sid")
		ctx.JSON(http.StatusBadRequest, gin.H{"error": "Call SID is required"})
		return
	}

	audioData, err := io.ReadAll(ctx.Request.Body)
	if err != nil {
		logger.Error("Failed to read audio data", "error", err)
		ctx.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to read audio data"})
		return
	}

	if len(audioData) == 0 {
		logger.Error("Received empty audio data", "call_sid", callSID)
		ctx.JSON(http.StatusBadRequest, gin.H{"error": "Audio data is empty"})
		return
	}

	// Store the audio in memory
	c.audioMutex.Lock()
	c.audioFiles[callSID] = audioData
	c.audioMutex.Unlock()

	// Set up a cleanup timer
	go func() {
		time.Sleep(15 * time.Minute) // Longer timeout to ensure it's available
		c.audioMutex.Lock()
		delete(c.audioFiles, callSID)
		c.audioMutex.Unlock()
		logger.Info("Cleaned up audio for call", "call_sid", callSID)
	}()

	logger.Info("Stored audio for call",
		"call_sid", callSID,
		"size", len(audioData),
		"content_type", ctx.GetHeader("Content-Type"))

	ctx.JSON(http.StatusOK, gin.H{
		"status":  "success",
		"message": "Audio stored successfully",
	})
}

// GetAudio retrieves audio data for a call
func (c *CallController) GetAudio(ctx *gin.Context) {
	callSID := ctx.Param("call_sid")
	if callSID == "" {
		logger.Error("Get audio request missing call_sid")
		ctx.JSON(http.StatusBadRequest, gin.H{"error": "Call SID is required"})
		return
	}

	c.audioMutex.RLock()
	audioData, exists := c.audioFiles[callSID]
	c.audioMutex.RUnlock()

	if !exists || len(audioData) == 0 {
		logger.Error("Audio not found or empty", "call_sid", callSID)
		ctx.JSON(http.StatusNotFound, gin.H{"error": "Audio not found"})
		return
	}

	logger.Info("Serving audio for call",
		"call_sid", callSID,
		"size", len(audioData))

	ctx.Header("Content-Type", "audio/mpeg")
	ctx.Header("Content-Length", fmt.Sprintf("%d", len(audioData)))
	ctx.Header("Cache-Control", "public, max-age=300") // Cache for 5 minutes

	ctx.Data(http.StatusOK, "audio/mpeg", audioData)
}

// HandleStreamingAudio processes audio stream data
func (c *CallController) HandleStreamingAudio(ctx *gin.Context) {
	callSID := ctx.Param("call_sid")
	if callSID == "" {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": "Call SID is required"})
		return
	}

	audioData, err := io.ReadAll(ctx.Request.Body)
	if err != nil {
		ctx.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to read audio data"})
		return
	}

	logger.Info("Received streaming audio", "call_sid", callSID, "size", len(audioData))

	ctx.Status(http.StatusOK)
}

// UploadRecordedFile uploads a recorded audio file for processing
func (c *CallController) UploadRecordedFile(ctx *gin.Context) {
	file, header, err := ctx.Request.FormFile("file")
	if err != nil {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": "No file provided"})
		return
	}
	defer file.Close()

	buffer := bytes.NewBuffer(nil)
	if _, err := io.Copy(buffer, file); err != nil {
		ctx.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to read file"})
		return
	}

	logger.Info("Received audio file upload", "filename", header.Filename, "size", header.Size)

	ctx.JSON(http.StatusOK, gin.H{
		"status":  "success",
		"message": "File processed successfully",
	})
}

// GetResponse streams the prepared AI response for a call
func (c *CallController) GetResponse(ctx *gin.Context) {
	callSID := ctx.Param("call_sid")
	if callSID == "" {
		logger.Error("Response request missing call_sid")
		ctx.Status(http.StatusBadRequest)
		return
	}

	// Check if we have audio stored for this call SID
	c.audioMutex.RLock()
	audioData, exists := c.audioFiles[callSID]
	c.audioMutex.RUnlock()

	// If audio exists and isn't empty, serve it
	if exists && len(audioData) > 0 {
		logger.Info("Serving stored audio response", "call_sid", callSID, "size", len(audioData))
		ctx.Header("Content-Type", "audio/mpeg")
		ctx.Header("Content-Length", fmt.Sprintf("%d", len(audioData)))
		ctx.Header("Cache-Control", "no-cache") // Prevent caching to ensure fresh responses
		ctx.Data(http.StatusOK, "audio/mpeg", audioData)
		return
	}

	// Attempt to get the text response while we wait for audio generation to complete
	aiResponse, err := c.callService.GetLastAIResponse(callSID)
	if err != nil || aiResponse == "" {
		aiResponse = "I'm processing your request. Let me continue with the next question."
	}

	// Wait briefly to see if audio becomes available (helps with race conditions)
	time.Sleep(500 * time.Millisecond)

	// Check again after waiting
	c.audioMutex.RLock()
	audioData, exists = c.audioFiles[callSID]
	c.audioMutex.RUnlock()

	if exists && len(audioData) > 0 {
		logger.Info("Serving stored audio response after brief wait", "call_sid", callSID, "size", len(audioData))
		ctx.Header("Content-Type", "audio/mpeg")
		ctx.Header("Content-Length", fmt.Sprintf("%d", len(audioData)))
		ctx.Header("Cache-Control", "no-cache")
		ctx.Data(http.StatusOK, "audio/mpeg", audioData)
		return
	}

	// If still no audio available, generate a simple response with TTS
	logger.Warn("No stored audio found after waiting, serving TTS fallback", "call_sid", callSID)
	ctx.Header("Content-Type", "text/plain")
	ctx.String(http.StatusOK, aiResponse)
}
