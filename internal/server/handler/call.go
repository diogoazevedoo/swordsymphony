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

	if speechInput == "" {
		speechInput = "No response provided"
	}

	callState, err := c.callService.GetCallState(callSID)
	if err != nil {
		logger.Error("Failed to get call state", "error", err)
		ctx.String(http.StatusInternalServerError, generateErrorTwiML("Technical difficulties. Please try again later."))
		return
	}

	if err := c.callService.GetConversationManager().AddMessage(
		callState.ConversationID,
		conversation.MessageTypePatient,
		speechInput); err != nil {
		logger.Error("Failed to add patient response", "error", err)
	}

	if err := c.callService.IncrementQuestionCounter(callSID); err != nil {
		logger.Error("Failed to increment question counter", "error", err)
	}

	twiML, err := c.callService.HandleInboundCall(ctx, twilio.CallEvent{
		CallSID: callSID,
		From:    callState.PatientPhone,
	})

	if err != nil {
		logger.Error("Failed to handle call", "error", err)
		ctx.String(http.StatusInternalServerError, generateErrorTwiML("Technical difficulties. Please try again later."))
		return
	}

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
	ctx.Header("Cache-Control", "public, max-age=60")

	ctx.Data(http.StatusOK, "audio/mpeg", audioData)
}

// generateErrorTwiML generates TwiML for error responses
func generateErrorTwiML(message string) string {
	return twilio.GenerateTwiML(
		twilio.SayAction(message, "alice", "en-US"),
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

	callState, err := c.callService.GetCallState(callSID)
	if err != nil {
		ctx.JSON(http.StatusNotFound, gin.H{
			"success": false,
			"error":   "Call not found or already processed",
		})
		return
	}

	conversationID := callState.ConversationID

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
