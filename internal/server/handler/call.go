package handler

import (
	"fmt"
	"net/http"

	"github.com/diogoazevedoo/swordsymphony/internal/communication/call"
	"github.com/diogoazevedoo/swordsymphony/internal/communication/twilio"
	"github.com/diogoazevedoo/swordsymphony/internal/logger"
	"github.com/gin-gonic/gin"
)

// CallController handles call-related HTTP endpoints
type CallController struct {
	callService *call.Service
}

// NewCallController creates a new call controller
func NewCallController(callService *call.Service) *CallController {
	return &CallController{
		callService: callService,
	}
}

// StartCall initiates a phone call to a patient
func (c *CallController) StartCall(ctx *gin.Context) {
	var request struct {
		PhoneNumber string `json:"phone_number" binding:"required"`
	}

	if err := ctx.ShouldBindJSON(&request); err != nil {
		ctx.JSON(http.StatusBadRequest, gin.H{
			"success": false,
			"error": gin.H{
				"message": "Invalid request",
				"details": err.Error(),
			},
		})
		return
	}

	callSID, err := c.callService.InitiateCall(request.PhoneNumber)
	if err != nil {
		ctx.JSON(http.StatusInternalServerError, gin.H{
			"success": false,
			"error": gin.H{
				"message": "Failed to start call",
				"details": err.Error(),
			},
		})
		return
	}

	ctx.JSON(http.StatusOK, gin.H{
		"success": true,
		"data": gin.H{
			"message":  "Call initiated successfully",
			"call_sid": callSID,
		},
	})
}

// EndCall terminates an active call
func (c *CallController) EndCall(ctx *gin.Context) {
	callSID := ctx.Param("call_sid")
	if callSID == "" {
		ctx.JSON(http.StatusBadRequest, gin.H{
			"success": false,
			"error": gin.H{
				"message": "Call SID is required",
			},
		})
		return
	}

	if err := c.callService.EndCall(callSID); err != nil {
		ctx.JSON(http.StatusInternalServerError, gin.H{
			"success": false,
			"error": gin.H{
				"message": "Failed to end call",
				"details": err.Error(),
			},
		})
		return
	}

	ctx.JSON(http.StatusOK, gin.H{
		"success": true,
		"data": gin.H{
			"message": "Call ended successfully",
		},
	})
}

// GetCallResults retrieves the results of a call
func (c *CallController) GetCallResults(ctx *gin.Context) {
	// This endpoint is not needed in the new implementation
	// since we process calls automatically after completion
	ctx.JSON(http.StatusOK, gin.H{
		"success": true,
		"data": gin.H{
			"message": "Call results are now processed automatically after call completion",
		},
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
		logger.Error("Speech callback missing call_sid")
		ctx.String(http.StatusBadRequest, generateErrorTwiML("Technical difficulties. Please try again later."))
		return
	}

	speechInput := ctx.PostForm("SpeechResult")
	logger.Info("Speech callback received",
		"call_sid", callSID,
		"input", speechInput,
		"input_length", len(speechInput))

	// Always continue even with empty input - just use a placeholder
	if speechInput == "" || len(speechInput) < 2 {
		speechInput = "No response provided" // Descriptive placeholder
		logger.Warn("Empty or very short speech input, using placeholder", 
			"call_sid", callSID,
			"original_input", speechInput)
	}

	// Process the speech input - this will also advance the question index
	// Even if there's an error, we still want the call to continue
	response, err := c.callService.ProcessSpeechInput(callSID, speechInput)
	if err != nil {
		logger.Error("Failed to process speech input, using fallback", 
			"error", err, 
			"call_sid", callSID)

		// Use a simple continuation message instead of stopping the flow
		response = "Let's continue to the next question."
		
		// Try to get the current state so we can continue
		// Any errors here are non-fatal - we'll still proceed with the simple response
		currentState, stateErr := c.callService.GetCallState(callSID)
		if stateErr == nil && currentState != nil {
			if nextQuestion, qErr := c.callService.GetNextQuestionForCall(callSID); qErr == nil && nextQuestion != "" {
				response = nextQuestion // Use the actual next question if available
			}
		}
	}

	// Try to generate audio, but have a solid fallback if it fails
	_, audioErr := c.callService.GenerateAudioResponse(callSID, response)
	if audioErr != nil {
		logger.Error("Failed to generate audio, using TTS fallback", 
			"error", audioErr, 
			"call_sid", callSID,
			"response_text", response)

		// Fall back to Twilio's text-to-speech with the response
		twiML := generateTwiMLWithSay(response, callSID, c.callService.GetBaseURL())
		ctx.Header("Content-Type", "text/xml")
		ctx.String(http.StatusOK, twiML)
		return
	}

	// Use the generated audio with longer timeouts for better user experience
	audioURL := c.callService.GetBaseURL() + "/api/call/audio/" + callSID
	twiML := generateTwiMLWithAudio(audioURL, callSID, c.callService.GetBaseURL())
	
	logger.Info("Sending audio response",
		"call_sid", callSID,
		"audio_url", audioURL,
		"response_text", response)

	ctx.Header("Content-Type", "text/xml")
	ctx.String(http.StatusOK, twiML)
}

// GetAudio retrieves audio data for a call
func (c *CallController) GetAudio(ctx *gin.Context) {
	callSID := ctx.Param("call_sid")
	if callSID == "" {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": "Call SID is required"})
		return
	}

	audioData, exists := c.callService.GetAudioResponse(callSID)
	if !exists || len(audioData) == 0 {
		logger.Error("Audio not found", "call_sid", callSID)
		ctx.JSON(http.StatusNotFound, gin.H{"error": "Audio not found"})
		return
	}

	ctx.Header("Content-Type", "audio/mpeg")
	ctx.Header("Content-Length", fmt.Sprintf("%d", len(audioData)))
	ctx.Header("Cache-Control", "public, max-age=60") // Cache for 1 minute

	ctx.Data(http.StatusOK, "audio/mpeg", audioData)
}

// StoreAudio stores audio data for a call
func (c *CallController) StoreAudio(ctx *gin.Context) {
	// This endpoint is not needed in the new implementation
	// since we store audio directly in the call service
	ctx.Status(http.StatusOK)
}

// HandleStreamingAudio processes streaming audio - not needed
func (c *CallController) HandleStreamingAudio(ctx *gin.Context) {
	// This endpoint is not needed in the new implementation
	ctx.Status(http.StatusOK)
}

// UploadRecordedFile uploads a recorded file - not needed
func (c *CallController) UploadRecordedFile(ctx *gin.Context) {
	// This endpoint is not needed in the new implementation
	ctx.Status(http.StatusOK)
}

// GetResponse streams the audio response - not needed
func (c *CallController) GetResponse(ctx *gin.Context) {
	// This endpoint is not needed in the new implementation
	ctx.Status(http.StatusOK)
}

// Helper functions

// generateErrorTwiML generates TwiML for error responses
func generateErrorTwiML(message string) string {
	return twilio.GenerateTwiML(
		twilio.SayAction(message, "alice", "en-US"),
	)
}

// generateRepeatTwiML generates TwiML asking the user to repeat
func generateRepeatTwiML(callSID, baseURL string) string {
	return twilio.GenerateTwiML(
		twilio.SayAction("I didn't catch that. Could you please respond to the question?", "alice", "en-US"),
		twilio.GatherAction("", map[string]string{
			"input":         "speech",
			"action":        fmt.Sprintf("%s/api/call/speech?call_sid=%s", baseURL, callSID),
			"language":      "en-US",
			"speechTimeout": "auto",
		}),
	)
}

// generateTwiMLWithSay generates TwiML with Say verb
func generateTwiMLWithSay(message, callSID, baseURL string) string {
	return twilio.GenerateTwiML(
		twilio.SayAction(message, "alice", "en-US"),
		twilio.GatherAction("", map[string]string{
			"input":         "speech",
			"action":        fmt.Sprintf("%s/api/call/speech?call_sid=%s", baseURL, callSID),
			"language":      "en-US",
			"speechTimeout": "5",  // 5 seconds of silence before timing out
			"timeout":       "15", // 15 seconds total for input
		}),
	)
}

// generateTwiMLWithAudio generates TwiML with Play verb for audio
func generateTwiMLWithAudio(audioURL, callSID, baseURL string) string {
	return twilio.GenerateTwiML(
		twilio.PlayAction(audioURL),
		twilio.GatherAction("", map[string]string{
			"input":         "speech",
			"action":        fmt.Sprintf("%s/api/call/speech?call_sid=%s", baseURL, callSID),
			"language":      "en-US",
			"speechTimeout": "5",  // 5 seconds of silence before timing out
			"timeout":       "15", // 15 seconds total for input
		}),
	)
}
