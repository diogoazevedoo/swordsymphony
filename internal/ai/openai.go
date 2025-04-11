package ai

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"image/jpeg"
	"io"
	"math/rand"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/diogoazevedoo/swordsymphony/internal/errors"
	"github.com/diogoazevedoo/swordsymphony/internal/logger"
	"github.com/gen2brain/go-fitz"
	"github.com/ledongthuc/pdf"
)

type openAIClient struct {
	apiKey     string
	httpClient *http.Client
	baseURL    string
	retries    int
}

// NewOpenAIClient creates a new OpenAI client
func NewOpenAIClient(apiKey string) Client {
	return &openAIClient{
		apiKey: apiKey,
		httpClient: &http.Client{
			Timeout: 60 * time.Second,
		},
		baseURL: "https://api.openai.com/v1",
		retries: 3,
	}
}

type openAIRequest struct {
	Model       string          `json:"model"`
	Messages    []openAIMessage `json:"messages"`
	MaxTokens   int             `json:"max_tokens,omitempty"`
	Temperature float64         `json:"temperature,omitempty"`
	TopP        float64         `json:"top_p,omitempty"`
}

type openAIMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type openAIResponse struct {
	Choices []struct {
		Message struct {
			Content string `json:"content"`
		} `json:"message"`
	} `json:"choices"`
	Error *struct {
		Message string `json:"message"`
		Type    string `json:"type"`
		Code    string `json:"code"`
	} `json:"error,omitempty"`
}

// GenerateCompletion implements the AI Client interface
func (c *openAIClient) GenerateCompletion(ctx context.Context, prompt string, options CompletionOptions) (CompletionResponse, error) {
	model := "gpt-4"
	if options.ModelName != "" {
		model = options.ModelName
	} else if strings.Contains(strings.ToLower(prompt), "conversation") ||
		strings.Contains(strings.ToLower(prompt), "patient") ||
		strings.Contains(strings.ToLower(prompt), "medical assistant") {
		model = "gpt-3.5-turbo"
		c.httpClient.Timeout = 15 * time.Second
	}

	messages := []openAIMessage{
		{Role: "user", Content: prompt},
	}

	if options.SystemPrompt != "" {
		messages = append([]openAIMessage{{Role: "system", Content: options.SystemPrompt}}, messages...)
	}

	reqBody := openAIRequest{
		Model:       model,
		Messages:    messages,
		MaxTokens:   options.MaxTokens,
		Temperature: options.Temperature,
		TopP:        options.TopP,
	}

	jsonData, err := json.Marshal(reqBody)
	if err != nil {
		return CompletionResponse{}, errors.Internal(fmt.Sprintf("failed to marshal request: %v", err), "ai_request_error")
	}

	return c.retryWithBackoff(ctx, jsonData, model)
}

func (c *openAIClient) doRequest(ctx context.Context, jsonData []byte, model string) (CompletionResponse, error) {
	req, err := http.NewRequestWithContext(ctx, "POST", c.baseURL+"/chat/completions", bytes.NewBuffer(jsonData))
	if err != nil {
		return CompletionResponse{}, errors.Internal(fmt.Sprintf("failed to create request: %v", err), "ai_request_error")
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", c.apiKey))

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return CompletionResponse{}, errors.External(err, "OpenAI API request failed", "ai_connection_error")
	}
	defer resp.Body.Close()

	var openAIResp openAIResponse
	if err := json.NewDecoder(resp.Body).Decode(&openAIResp); err != nil {
		return CompletionResponse{}, errors.External(err, "Failed to decode OpenAI response", "ai_decode_error")
	}

	if openAIResp.Error != nil {
		return CompletionResponse{}, errors.External(
			fmt.Errorf("%s: %s", openAIResp.Error.Type, openAIResp.Error.Message),
			"OpenAI API returned an error",
			openAIResp.Error.Code,
		)
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return CompletionResponse{}, errors.External(
			fmt.Errorf("status code %d", resp.StatusCode),
			"OpenAI API returned an unexpected status code",
			"ai_status_error",
		)
	}

	if len(openAIResp.Choices) == 0 {
		return CompletionResponse{}, errors.External(
			fmt.Errorf("no choices returned"),
			"OpenAI API returned no completion choices",
			"ai_empty_response",
		)
	}

	return CompletionResponse{
		Text: openAIResp.Choices[0].Message.Content,
		Metadata: map[string]string{
			"model": model,
		},
	}, nil
}

// isTransientError determines if an error is likely temporary and should be retried
func isTransientError(err error) bool {
	if appErr, ok := err.(*errors.AppError); ok {
		if appErr.Type == errors.ErrorTypeExternal {
			if appErr.Code == "rate_limit_exceeded" ||
				appErr.Code == "server_error" ||
				appErr.Code == "service_unavailable" ||
				strings.Contains(appErr.Message, "timeout") ||
				strings.Contains(appErr.Message, "connection") {
				return true
			}
		}
	}
	return false
}

// GenerateEmbedding creates vector embeddings
func (c *openAIClient) GenerateEmbedding(ctx context.Context, text string) ([]float64, error) {
	reqBody := map[string]any{
		"model": "text-embedding-ada-002",
		"input": text,
	}

	jsonData, err := json.Marshal(reqBody)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, "POST", c.baseURL+"/embeddings", bytes.NewBuffer(jsonData))
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", c.apiKey))

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("OpenAI API request failed: %w", err)
	}
	defer resp.Body.Close()

	var result struct {
		Data []struct {
			Embedding []float64 `json:"embedding"`
		} `json:"data"`
		Error *struct {
			Message string `json:"message"`
			Type    string `json:"type"`
			Code    string `json:"code"`
		} `json:"error,omitempty"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	if result.Error != nil {
		return nil, fmt.Errorf("OpenAI API error: %s (%s)", result.Error.Message, result.Error.Type)
	}

	if len(result.Data) == 0 || len(result.Data[0].Embedding) == 0 {
		return nil, fmt.Errorf("no embedding returned")
	}

	return result.Data[0].Embedding, nil
}

// retryWithBackoff retries a request with exponential backoff
func (c *openAIClient) retryWithBackoff(ctx context.Context, jsonData []byte, model string) (CompletionResponse, error) {
	var result CompletionResponse
	var lastErr error

	for attempt := 0; attempt <= c.retries; attempt++ {
		if attempt > 0 {
			backoffDuration := time.Duration(attempt*attempt) * 500 * time.Millisecond
			jitter := time.Duration(rand.Intn(100)) * time.Millisecond
			backoffWithJitter := backoffDuration + jitter

			logger.Info("Retrying OpenAI API request",
				"attempt", attempt,
				"max_retries", c.retries,
				"backoff", backoffWithJitter.String())

			select {
			case <-time.After(backoffWithJitter):
				// Continue with retry
			case <-ctx.Done():
				return CompletionResponse{}, fmt.Errorf("context cancelled during backoff: %w", ctx.Err())
			}
		}

		result, lastErr = c.doRequest(ctx, jsonData, model)
		if lastErr == nil {
			return result, nil
		}

		if !isTransientError(lastErr) {
			return CompletionResponse{}, lastErr
		}

		logger.Warn("Transient error from OpenAI API, will retry",
			"attempt", attempt+1,
			"max_retries", c.retries,
			"error", lastErr)
	}

	return CompletionResponse{}, fmt.Errorf("failed after %d retries: %w", c.retries, lastErr)
}

// AnalyzeDocument analyzes a document using the appropriate OpenAI API
func (c *openAIClient) AnalyzeDocument(ctx context.Context, filePath string, contentType string, documentType string) (map[string]any, error) {
	if strings.Contains(contentType, "pdf") || strings.HasSuffix(filePath, ".pdf") {
		return c.analyzePDFDocument(ctx, filePath, documentType)
	}

	return nil, fmt.Errorf("unsupported document type: %s", contentType)
}

// analyzePDFDocument analyzes a PDF document using GPT-4
func (c *openAIClient) analyzePDFDocument(ctx context.Context, filePath string, documentType string) (map[string]any, error) {
	extractedText, err := extractTextFromPDF(filePath)
	if err != nil {
		logger.Error("Failed to extract text from PDF", "path", filePath, "error", err)
		logger.Info("Attempting vision-based analysis as fallback", "path", filePath)
		return c.analyzeDocumentWithVision(ctx, filePath, documentType)
	}

	if len(extractedText) == 0 || strings.TrimSpace(extractedText) == "" {
		logger.Warn("No text could be extracted from PDF, attempting vision-based analysis", "path", filePath)
		return c.analyzeDocumentWithVision(ctx, filePath, documentType)
	}

	prompt := c.getPromptForDocumentType(documentType)

	messages := []openAIMessage{
		{
			Role:    "system",
			Content: prompt,
		},
		{
			Role: "user",
			Content: fmt.Sprintf("This is a %s document. Please analyze the following text extracted from the document:\n\n%s",
				documentType, truncateContent(extractedText, 6000)),
		},
	}

	reqBody := openAIRequest{
		Model:    "gpt-4",
		Messages: messages,
	}

	requestBody, err := json.Marshal(reqBody)
	if err != nil {
		return nil, fmt.Errorf("failed to create request body: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, "POST", c.baseURL+"/chat/completions", bytes.NewBuffer(requestBody))
	if err != nil {
		return nil, fmt.Errorf("failed to create HTTP request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+c.apiKey)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to send request to OpenAI: %w", err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response body: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("OpenAI API error: %s - %s", resp.Status, string(respBody))
	}

	var apiResponse map[string]any
	if err := json.Unmarshal(respBody, &apiResponse); err != nil {
		return nil, fmt.Errorf("failed to parse API response: %w", err)
	}

	choices, ok := apiResponse["choices"].([]any)
	if !ok || len(choices) == 0 {
		return nil, fmt.Errorf("invalid API response format")
	}

	choice, ok := choices[0].(map[string]any)
	if !ok {
		return nil, fmt.Errorf("invalid choice format in API response")
	}

	message, ok := choice["message"].(map[string]any)
	if !ok {
		return nil, fmt.Errorf("invalid message format in API response")
	}

	content, ok := message["content"].(string)
	if !ok {
		return nil, fmt.Errorf("invalid content format in API response")
	}

	var analysisResult map[string]any
	jsonStart := strings.Index(content, "{")
	jsonEnd := strings.LastIndex(content, "}")
	if jsonStart >= 0 && jsonEnd > jsonStart {
		jsonStr := content[jsonStart : jsonEnd+1]
		if err := json.Unmarshal([]byte(jsonStr), &analysisResult); err != nil {
			logger.Warn("Failed to parse JSON in OpenAI response", "error", err)
			analysisResult = map[string]any{
				"text":       content,
				"confidence": 0.7,
				"error":      "Failed to parse response as JSON",
			}
		}
	} else {
		analysisResult = map[string]any{
			"text":       content,
			"confidence": 0.7,
		}
	}

	return analysisResult, nil
}

// analyzeDocumentWithVision performs analysis using GPT-4o's vision capabilities when text extraction fails
func (c *openAIClient) analyzeDocumentWithVision(ctx context.Context, filePath string, documentType string) (map[string]any, error) {
	imagePaths, err := convertPDFToImages(filePath)
	if err != nil {
		logger.Error("Failed to convert PDF to images", "path", filePath, "error", err)
		return map[string]any{
			"error":      fmt.Sprintf("Failed to convert PDF to images: %v", err),
			"confidence": 0.0,
			"text":       "Could not convert the PDF to images for analysis.",
		}, nil
	}
	defer func() {
		for _, path := range imagePaths {
			os.Remove(path)
		}
	}()

	combinedResult := map[string]any{
		"pages":      []map[string]any{},
		"confidence": 0.0,
		"method":     "vision",
	}

	for pageIndex, imagePath := range imagePaths {
		imageData, err := os.ReadFile(imagePath)
		if err != nil {
			logger.Error("Failed to read converted image", "path", imagePath, "error", err)
			continue
		}

		base64Data := base64.StdEncoding.EncodeToString(imageData)

		visPrompt := c.getVisionPromptForDocumentType(documentType)

		messages := []map[string]interface{}{
			{
				"role":    "system",
				"content": visPrompt,
			},
			{
				"role": "user",
				"content": []map[string]interface{}{
					{
						"type": "text",
						"text": fmt.Sprintf("Please analyze this %s document and respond with valid JSON ONLY. This is page %d of a PDF document.", documentType, pageIndex+1),
					},
					{
						"type": "image_url",
						"image_url": map[string]interface{}{
							"url":    fmt.Sprintf("data:image/jpeg;base64,%s", base64Data),
							"detail": "high",
						},
					},
				},
			},
		}

		result, err := c.sendVisionRequest(ctx, "gpt-4o", messages)
		if err != nil {
			logger.Warn("GPT-4o vision analysis failed, falling back to GPT-4o-mini", "page", pageIndex+1, "error", err)
			result, err = c.sendVisionRequest(ctx, "gpt-4o-mini", messages)
			if err != nil {
				logger.Error("Vision model failed for page", "page", pageIndex+1, "error", err)
				continue
			}
		}

		result["page_number"] = pageIndex + 1

		if pageResults, ok := combinedResult["pages"].([]map[string]any); ok {
			combinedResult["pages"] = append(pageResults, result)
		}

		if conf, ok := result["confidence"].(float64); ok {
			currentConf := combinedResult["confidence"].(float64)
			numPages := len(combinedResult["pages"].([]map[string]any))
			combinedResult["confidence"] = (currentConf*float64(numPages-1) + conf) / float64(numPages)
		}
	}

	if len(combinedResult["pages"].([]map[string]any)) == 0 {
		return map[string]any{
			"error":      "Failed to analyze any page of the PDF document",
			"confidence": 0.0,
			"method":     "vision",
		}, nil
	}

	return combinedResult, nil
}

// convertPDFToImages converts all pages of a PDF file to JPEG images
func convertPDFToImages(pdfPath string) ([]string, error) {
	tmpDir := filepath.Join(os.TempDir(), "pdf_images")
	if err := os.MkdirAll(tmpDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create temporary directory: %w", err)
	}

	doc, err := fitz.New(pdfPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open PDF with fitz: %w", err)
	}
	defer doc.Close()

	totalPages := doc.NumPage()
	if totalPages < 1 {
		return nil, fmt.Errorf("PDF has no pages")
	}

	maxPages := 10
	if totalPages > maxPages {
		logger.Warn("PDF has too many pages, limiting analysis", "total_pages", totalPages, "max_pages", maxPages)
		totalPages = maxPages
	}

	imagePaths := make([]string, 0, totalPages)

	for pageIndex := 0; pageIndex < totalPages; pageIndex++ {
		outputImagePath := filepath.Join(tmpDir, fmt.Sprintf("%d_page_%d.jpg", time.Now().UnixNano(), pageIndex+1))

		img, err := doc.Image(pageIndex)
		if err != nil {
			logger.Warn("Failed to extract image from PDF page", "page", pageIndex+1, "error", err)
			continue
		}

		file, err := os.Create(outputImagePath)
		if err != nil {
			logger.Warn("Failed to create output file for page", "page", pageIndex+1, "error", err)
			continue
		}

		err = jpeg.Encode(file, img, &jpeg.Options{Quality: 90})
		file.Close()
		if err != nil {
			logger.Warn("Failed to encode image for page", "page", pageIndex+1, "error", err)
			os.Remove(outputImagePath)
			continue
		}

		logger.Info("Successfully converted PDF page to image", "pdf", pdfPath, "page", pageIndex+1, "image", outputImagePath)
		imagePaths = append(imagePaths, outputImagePath)
	}

	if len(imagePaths) == 0 {
		return nil, fmt.Errorf("failed to convert any page of the PDF to images")
	}

	return imagePaths, nil
}

// sendVisionRequest sends a request to the specified vision model and processes the response
func (c *openAIClient) sendVisionRequest(ctx context.Context, model string, messages []map[string]interface{}) (map[string]any, error) {
	visionRequest := map[string]interface{}{
		"model":      model,
		"messages":   messages,
		"max_tokens": 1000,
	}

	requestBody, err := json.Marshal(visionRequest)
	if err != nil {
		return nil, fmt.Errorf("failed to create vision request body: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, "POST", c.baseURL+"/chat/completions", bytes.NewBuffer(requestBody))
	if err != nil {
		return nil, fmt.Errorf("failed to create HTTP request for vision API: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+c.apiKey)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to send request to OpenAI vision API: %w", err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read vision API response body: %w", err)
	}

	logger.Info("Vision API response", "model", model, "status", resp.Status)
	if len(respBody) > 200 {
		logger.Debug("Vision API response preview", "body", string(respBody[:200])+"...")
	} else {
		logger.Debug("Vision API response", "body", string(respBody))
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("OpenAI vision API error: %s - %s", resp.Status, string(respBody))
	}

	var apiResponse map[string]any
	if err := json.Unmarshal(respBody, &apiResponse); err != nil {
		return nil, fmt.Errorf("failed to parse vision API response: %w", err)
	}

	choices, ok := apiResponse["choices"].([]any)
	if !ok || len(choices) == 0 {
		return nil, fmt.Errorf("invalid vision API response format")
	}

	choice, ok := choices[0].(map[string]any)
	if !ok {
		return nil, fmt.Errorf("invalid choice format in vision API response")
	}

	message, ok := choice["message"].(map[string]any)
	if !ok {
		return nil, fmt.Errorf("invalid message format in vision API response")
	}

	content, ok := message["content"].(string)
	if !ok {
		return nil, fmt.Errorf("invalid content format in vision API response")
	}

	var analysisResult map[string]any
	jsonStart := strings.Index(content, "{")
	jsonEnd := strings.LastIndex(content, "}")
	if jsonStart >= 0 && jsonEnd > jsonStart {
		jsonStr := content[jsonStart : jsonEnd+1]
		if err := json.Unmarshal([]byte(jsonStr), &analysisResult); err != nil {
			logger.Warn("Failed to parse JSON in vision API response", "error", err)
			analysisResult = map[string]any{
				"text":       content,
				"confidence": 0.6,
				"method":     "vision",
				"error":      "Failed to parse vision response as JSON",
			}
		} else {
			analysisResult["method"] = "vision"
			analysisResult["model"] = model
		}
	} else {
		analysisResult = map[string]any{
			"text":       content,
			"confidence": 0.6,
			"method":     "vision",
			"model":      model,
		}
	}

	return analysisResult, nil
}

// extractTextFromPDF extracts text content from a PDF file
func extractTextFromPDF(filePath string) (string, error) {
	f, r, err := pdf.Open(filePath)
	if err != nil {
		return "", fmt.Errorf("error opening PDF: %w", err)
	}
	defer f.Close()

	totalPages := r.NumPage()

	var textBuilder strings.Builder

	for pageIndex := 1; pageIndex <= totalPages; pageIndex++ {
		p := r.Page(pageIndex)
		if p.V.IsNull() {
			continue
		}

		text, err := p.GetPlainText(nil)
		if err != nil {
			logger.Warn("Error extracting text from page", "page", pageIndex, "error", err)
			continue
		}

		textBuilder.WriteString(text)
		textBuilder.WriteString("\n--- Page Break ---\n")
	}

	return textBuilder.String(), nil
}

// truncateContent limits the content to a specified maximum length
func truncateContent(content string, maxLength int) string {
	if len(content) <= maxLength {
		return content
	}

	truncated := content[:maxLength]

	truncated += fmt.Sprintf("\n\n[Note: The document is longer than %d characters. This is a truncated version.]", maxLength)

	return truncated
}

// documentPrompts contains the prompts for different document types
var documentPrompts = map[string]string{
	"lab_result": `You are a medical AI assistant analyzing lab results. 
Extract key information in a structured format from the lab document provided.
Focus on test names, values, normal ranges, and abnormal results.
The document content might be base64 encoded - if it appears to be encoded and you can't meaningfully analyze it,
return a clear error message but maintain proper JSON format with default values.

Return the analysis as a JSON object with these fields:
- key_values: Map of test names to their values
- abnormal_values: Array of test names with abnormal results
- findings: Array of observations based on the results
- interpretation: Overall medical interpretation of the results
- confidence: Your confidence in the analysis (0-1)

Your response must be valid JSON. Begin with { and end with }.`,

	"xray": `You are a medical AI assistant analyzing X-ray images.
Provide detailed analysis of the X-ray image described or encoded.
Focus on bone structure, fractures, calcifications, and other visible abnormalities.
The document content might be base64 encoded - this is expected for images, but
if you can't meaningfully analyze it, return a clear error message in proper JSON format.

Return the analysis as a JSON object with these fields:
- findings: Array of observations
- abnormalities: Array of detected issues
- potential_diagnoses: Array of possible conditions
- significance: Overall medical significance
- confidence: Your confidence in the analysis (0-1)

Your response must be valid JSON. Begin with { and end with }.`,

	"default": `You are a medical AI assistant analyzing a medical document.
Extract key information from the document in a structured format.
Identify any abnormal findings, diagnoses, or important medical information.
The document content might be base64 encoded - if it appears to be encoded and you can't meaningfully analyze it,
return a clear error message but maintain proper JSON format with default values.

Return the analysis as a JSON object with these fields:
- findings: Array of key findings from the document
- abnormalities: Array of abnormal results or findings
- interpretation: Overall interpretation of the document
- confidence: Your confidence in the analysis (0-1)

Your response must be valid JSON. Begin with { and end with }.`,
}

// getPromptForDocumentType returns the appropriate prompt for the document type
func (c *openAIClient) getPromptForDocumentType(documentType string) string {
	prompt, exists := documentPrompts[documentType]
	if !exists {
		return documentPrompts["default"]
	}
	return prompt
}

// getVisionPromptForDocumentType returns prompts optimized for the vision model
func (c *openAIClient) getVisionPromptForDocumentType(documentType string) string {
	visionPrompts := map[string]string{
		"lab_result": `You are a medical AI assistant that can analyze medical lab results from images or PDFs.
Examine the document carefully and extract all the information you can see.
Focus on test names, values, normal ranges, and abnormal results.

Return ONLY valid JSON with these fields (no other text):
- key_values: Map of test names to their values
- abnormal_values: Array of test names with abnormal results
- findings: Array of observations based on the results
- interpretation: Overall medical interpretation of the results
- confidence: Your confidence in the analysis (0-1)

If you can't analyze the document at all, return JSON with:
- error: A description of why you couldn't analyze it
- confidence: 0`,

		"xray": `You are a medical AI assistant that can analyze X-ray images.
Examine the document carefully.
Focus on bone structure, fractures, calcifications, and other visible abnormalities.

Return ONLY valid JSON with these fields (no other text):
- findings: Array of observations
- abnormalities: Array of detected issues
- potential_diagnoses: Array of possible conditions
- significance: Overall medical significance
- confidence: Your confidence in the analysis (0-1)

If you can't analyze the document at all, return JSON with:
- error: A description of why you couldn't analyze it
- confidence: 0`,

		"default": `You are a medical AI assistant that can analyze medical documents from images or PDFs.
Examine the document carefully and extract all the information you can see.
Identify any abnormal findings, diagnoses, or important medical information.

Return ONLY valid JSON with these fields (no other text):
- findings: Array of key findings from the document
- abnormalities: Array of abnormal results or findings
- interpretation: Overall interpretation of the document
- confidence: Your confidence in the analysis (0-1)

If you can't analyze the document at all, return JSON with:
- error: A description of why you couldn't analyze it
- confidence: 0`,
	}

	prompt, exists := visionPrompts[documentType]
	if !exists {
		return visionPrompts["default"]
	}
	return prompt
}
