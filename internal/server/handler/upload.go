package handler

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/diogoazevedoo/swordsymphony/internal/errors"
	"github.com/diogoazevedoo/swordsymphony/internal/server/response"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
)

// UploadPatientData handles file uploads for patient data
func (h *ActorHandler) UploadPatientData(c *gin.Context) {
	if err := c.Request.ParseMultipartForm(10 << 20); err != nil { // 10 MB max
		response.Error(c, errors.Validation("Failed to parse upload: too large", "file_too_large"))
		return
	}

	file, header, err := c.Request.FormFile("file")
	if err != nil {
		response.Error(c, errors.Validation("No file provided", "no_file"))
		return
	}
	defer file.Close()

	if header.Size > 10<<20 { // 10 MB
		response.Error(c, errors.Validation("File too large", "file_too_large"))
		return
	}

	filename := header.Filename
	fileExt := strings.ToLower(filepath.Ext(filename))

	var patientData map[string]any

	switch fileExt {
	case ".json":
		patientData, err = processJSONFile(file)
	case ".csv":
		patientData, err = processCSVFile(file)
	default:
		response.Error(c, errors.Validation("Unsupported file type", "unsupported_file_type"))
		return
	}

	if err != nil {
		response.Error(c, err)
		return
	}

	if _, exists := patientData["id"]; !exists {
		patientData["id"] = uuid.New().String()
	}

	caseID := patientData["id"].(string)
	err = h.caseRepository.StoreCase(caseID, patientData, false)
	if err != nil {
		response.Error(c, err)
		return
	}

	response.JSON(c, gin.H{
		"message":  "File uploaded and processed successfully",
		"case_id":  caseID,
		"filename": filename,
	})
}

// processJSONFile processes a JSON file into patient data
func processJSONFile(file io.Reader) (map[string]any, error) {
	var patientData map[string]any

	data, err := io.ReadAll(file)
	if err != nil {
		return nil, errors.Wrap(err, errors.ErrorTypeInternal, "Failed to read file", "file_read_error")
	}

	if err := json.Unmarshal(data, &patientData); err != nil {
		return nil, errors.Wrap(err, errors.ErrorTypeValidation, "Invalid JSON format", "json_parse_error")
	}

	requiredFields := []string{"name", "age", "gender", "symptoms"}
	for _, field := range requiredFields {
		if _, exists := patientData[field]; !exists {
			return nil, errors.Validation(
				fmt.Sprintf("Missing required field: %s", field),
				"missing_required_field",
			)
		}
	}

	return patientData, nil
}

// processCSVFile processes a CSV file into patient data
func processCSVFile(file io.Reader) (map[string]any, error) {
	reader := csv.NewReader(file)
	records, err := reader.ReadAll()
	if err != nil {
		return nil, errors.Wrap(err, errors.ErrorTypeValidation, "Failed to parse CSV", "csv_parse_error")
	}

	if len(records) < 2 {
		return nil, errors.Validation("CSV file must contain headers and at least one row of data", "invalid_csv")
	}

	headers := records[0]
	data := records[1]

	if len(headers) != len(data) {
		return nil, errors.Validation("CSV data row does not match headers", "mismatched_csv")
	}

	patientData := make(map[string]any)

	for i, header := range headers {
		header = strings.TrimSpace(strings.ToLower(header))
		value := strings.TrimSpace(data[i])

		switch header {
		case "id", "name", "gender":
			patientData[header] = value
		case "age":
			age, err := strconv.Atoi(value)
			if err != nil {
				return nil, errors.Validation("Age must be a number", "invalid_age")
			}
			patientData[header] = age
		case "symptoms", "conditions", "medications", "allergies":
			items := make([]string, 0)
			for _, item := range strings.Split(value, ",") {
				trimmed := strings.TrimSpace(item)
				if trimmed != "" {
					items = append(items, trimmed)
				}
			}
			patientData[header] = items
		default:
			patientData[header] = value
		}
	}

	requiredFields := []string{"name", "age", "gender", "symptoms"}
	for _, field := range requiredFields {
		if _, exists := patientData[field]; !exists {
			return nil, errors.Validation(
				fmt.Sprintf("Missing required field: %s", field),
				"missing_required_field",
			)
		}
	}

	return patientData, nil
}
