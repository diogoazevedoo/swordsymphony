package handler

import (
	"fmt"
	"net/http"

	"github.com/diogoazevedoo/swordsymphony/internal/communication/call"
	"github.com/diogoazevedoo/swordsymphony/internal/communication/twilio"
	"github.com/diogoazevedoo/swordsymphony/internal/conversation"
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

// HandleSpeechCallback processes speech input from Twilio for inbound calls
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
		"input", speechInput)

	// Always continue even with empty input
	if speechInput == "" {
		speechInput = "No response provided"
	}

	// Get the call state
	callState, err := c.callService.GetCallState(callSID)
	if err != nil {
		logger.Error("Failed to get call state", "error", err)
		ctx.String(http.StatusInternalServerError, generateErrorTwiML("Technical difficulties. Please try again later."))
		return
	}

	// Store the patient's response
	if err := c.callService.GetConversationManager().AddMessage(
		callState.ConversationID,
		conversation.MessageTypePatient,
		speechInput); err != nil {
		logger.Error("Failed to add patient response", "error", err)
	}

	// Increment the question counter to move to the next question
	if err := c.callService.IncrementQuestionCounter(callSID); err != nil {
		logger.Error("Failed to increment question counter", "error", err)
	}

	// Generate the next response using the inbound call handler
	twiML, err := c.callService.HandleInboundCall(ctx, twilio.CallEvent{
		CallSID: callSID,
		From:    callState.PatientPhone,
	})

	if err != nil {
		logger.Error("Failed to handle call", "error", err)
		ctx.String(http.StatusInternalServerError, generateErrorTwiML("Technical difficulties. Please try again later."))
		return
	}

	// Return the TwiML response
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

// StartSimpleCall initiates a simplified call flow
func (c *CallController) StartSimpleCall(ctx *gin.Context) {
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

	conversationID, err := c.callService.SimpleConversationFlow(request.PhoneNumber)
	if err != nil {
		ctx.JSON(http.StatusInternalServerError, gin.H{
			"success": false,
			"error": gin.H{
				"message": "Failed to start simple call flow",
				"details": err.Error(),
			},
		})
		return
	}

	ctx.JSON(http.StatusOK, gin.H{
		"success": true,
		"data": gin.H{
			"message":         "Simple call flow initiated successfully",
			"conversation_id": conversationID,
		},
	})
}

// ProcessCallToCase processes a completed call into a case
func (c *CallController) ProcessCallToCase(ctx *gin.Context) {
	conversationID := ctx.Param("conversation_id")
	if conversationID == "" {
		ctx.JSON(http.StatusBadRequest, gin.H{
			"success": false,
			"error": gin.H{
				"message": "Conversation ID is required",
			},
		})
		return
	}

	caseID, err := c.callService.ProcessConversationToCase(conversationID)
	if err != nil {
		ctx.JSON(http.StatusInternalServerError, gin.H{
			"success": false,
			"error": gin.H{
				"message": "Failed to process conversation to case",
				"details": err.Error(),
			},
		})
		return
	}

	ctx.JSON(http.StatusOK, gin.H{
		"success": true,
		"data": gin.H{
			"message": "Conversation processed successfully",
			"case_id": caseID,
		},
	})
}

// ProcessCallConversation manually processes a call conversation into a patient case
func (c *CallController) ProcessCallConversation(ctx *gin.Context) {
	callSID := ctx.Param("call_sid")
	if callSID == "" {
		ctx.JSON(http.StatusBadRequest, gin.H{
			"success": false,
			"error":   "Call SID is required",
		})
		return
	}

	// Get the conversation ID from the call
	callState, err := c.callService.GetCallState(callSID)
	if err != nil {
		ctx.JSON(http.StatusNotFound, gin.H{
			"success": false,
			"error":   "Call not found or already processed",
		})
		return
	}

	conversationID := callState.ConversationID

	// Process the conversation
	processResult, err := c.callService.ProcessConversationToCase(conversationID)
	if err != nil {
		ctx.JSON(http.StatusInternalServerError, gin.H{
			"success": false,
			"error":   "Failed to process conversation: " + err.Error(),
		})
		return
	}

	ctx.JSON(http.StatusOK, gin.H{
		"success": true,
		"data": gin.H{
			"message":         "Call processed successfully",
			"case_id":         processResult,
			"conversation_id": conversationID,
		},
	})
}
