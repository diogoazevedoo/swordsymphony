package handler

import (
	"bytes"
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

// HandleSpeechCallback processes speech input from Twilio
func (c *CallController) HandleSpeechCallback(ctx *gin.Context) {
	callSID := ctx.Query("call_sid")
	if callSID == "" {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": "Call SID is required"})
		return
	}

	speechInput := ctx.PostForm("SpeechResult")
	if speechInput == "" {
		twiml := `<?xml version="1.0" encoding="UTF-8"?><Response>
			<Say voice="alice">I'm sorry, I didn't catch that. Could you please repeat?</Say>
			<Gather input="speech" action="/api/call/speech?call_sid=` + callSID + `" language="en-US" speechTimeout="auto"/>
		</Response>`

		ctx.Header("Content-Type", "text/xml")
		ctx.String(http.StatusOK, twiml)
		return
	}

	logger.Info("Received speech input", "call_sid", callSID, "input", speechInput)

	aiResponse, err := c.callService.HandleSpeechInput(callSID, speechInput)
	if err != nil {
		logger.Error("Error handling speech input", "error", err)

		twiml := `<?xml version="1.0" encoding="UTF-8"?><Response>
			<Say voice="alice">I'm having trouble processing that. Let me try again.</Say>
			<Gather input="speech" action="/api/call/speech?call_sid=` + callSID + `" language="en-US" speechTimeout="auto"/>
		</Response>`

		ctx.Header("Content-Type", "text/xml")
		ctx.String(http.StatusOK, twiml)
		return
	}

	_, err = c.callService.GenerateAndStoreAudioResponse(callSID, aiResponse)
	if err != nil {
		// If audio generation fails, fall back to Twilio's voice
		twiml := `<?xml version="1.0" encoding="UTF-8"?><Response>
            <Say voice="alice">` + aiResponse + `</Say>
            <Gather input="speech" action="/api/call/speech?call_sid=` + callSID + `" language="en-US" speechTimeout="auto"/>
        </Response>`

		ctx.Header("Content-Type", "text/xml")
		ctx.String(http.StatusOK, twiml)
		return
	}

	audioURL := c.callService.GetBaseURL() + "/api/call/audio/" + callSID

	twiml := `<?xml version="1.0" encoding="UTF-8"?><Response>
        <Play>` + audioURL + `</Play>
        <Gather input="speech" action="/api/call/speech?call_sid=` + callSID + `" language="en-US" speechTimeout="auto"/>
    </Response>`

	ctx.Header("Content-Type", "text/xml")
	ctx.String(http.StatusOK, twiml)
}

func (c *CallController) StoreAudio(ctx *gin.Context) {
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

	// Store the audio in memory
	c.audioMutex.Lock()
	c.audioFiles[callSID] = audioData
	c.audioMutex.Unlock()

	// Set up a cleanup timer
	go func() {
		time.Sleep(5 * time.Minute)
		c.audioMutex.Lock()
		delete(c.audioFiles, callSID)
		c.audioMutex.Unlock()
	}()

	logger.Info("Stored audio for call", "call_sid", callSID, "size", len(audioData))
	ctx.JSON(http.StatusOK, gin.H{
		"status":  "success",
		"message": "Audio stored successfully",
	})
}

// GetAudio retrieves audio data for a call
func (c *CallController) GetAudio(ctx *gin.Context) {
	callSID := ctx.Param("call_sid")
	if callSID == "" {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": "Call SID is required"})
		return
	}

	c.audioMutex.RLock()
	audioData, exists := c.audioFiles[callSID]
	c.audioMutex.RUnlock()

	if !exists {
		ctx.JSON(http.StatusNotFound, gin.H{"error": "Audio not found"})
		return
	}

	ctx.Header("Content-Type", "audio/mpeg")
	ctx.Header("Content-Length", fmt.Sprintf("%d", len(audioData)))

	ctx.Writer.Write(audioData)
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
