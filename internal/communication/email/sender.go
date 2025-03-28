package email

import (
	"bytes"
	"encoding/base64"
	"fmt"
	"html/template"
	"net/smtp"
	"path/filepath"
	"strings"
	"time"

	"github.com/diogoazevedoo/swordsymphony/internal/logger"
)

// Sender represents an email sender
type Sender struct {
	smtpServer  string
	smtpPort    int
	username    string
	password    string
	fromEmail   string
	fromName    string
	templateDir string
	templates   map[string]*template.Template
}

// EmailContent contains the data for an email
type EmailContent struct {
	To           string
	Subject      string
	Body         string
	HTMLBody     string
	Template     string
	TemplateData map[string]any
	Attachments  []Attachment
}

// Attachment represents a file attachment
type Attachment struct {
	Filename    string
	ContentType string
	Data        []byte
}

// NewSender creates a new email sender
func NewSender(smtpServer string, smtpPort int, username, password, fromEmail, fromName, templateDir string) *Sender {
	return &Sender{
		smtpServer:  smtpServer,
		smtpPort:    smtpPort,
		username:    username,
		password:    password,
		fromEmail:   fromEmail,
		fromName:    fromName,
		templateDir: templateDir,
		templates:   make(map[string]*template.Template),
	}
}

// LoadTemplates loads email templates from the template directory
func (s *Sender) LoadTemplates() error {
	if s.templateDir == "" {
		return nil
	}

	htmlFiles, err := filepath.Glob(filepath.Join(s.templateDir, "*.html"))
	if err != nil {
		return fmt.Errorf("error finding HTML templates: %w", err)
	}

	for _, file := range htmlFiles {
		name := filepath.Base(file)
		tmpl, err := template.ParseFiles(file)
		if err != nil {
			return fmt.Errorf("error parsing template %s: %w", name, err)
		}
		s.templates[name] = tmpl
	}

	logger.Info("Loaded email templates", "count", len(s.templates))
	return nil
}

// Send sends an email
func (s *Sender) Send(content EmailContent) error {
	if content.Template != "" && content.HTMLBody == "" {
		tmpl, exists := s.templates[content.Template]
		if !exists {
			return fmt.Errorf("template %s not found", content.Template)
		}

		var htmlBuf bytes.Buffer
		if err := tmpl.Execute(&htmlBuf, content.TemplateData); err != nil {
			return fmt.Errorf("error executing template: %w", err)
		}
		content.HTMLBody = htmlBuf.String()
	}

	header := make(map[string]string)
	header["From"] = fmt.Sprintf("%s <%s>", s.fromName, s.fromEmail)
	header["To"] = content.To
	header["Subject"] = content.Subject
	header["MIME-Version"] = "1.0"
	boundary := fmt.Sprintf("boundary-%d", time.Now().UnixNano())
	header["Content-Type"] = fmt.Sprintf("multipart/mixed; boundary=%s", boundary)
	var message bytes.Buffer
	for key, value := range header {
		message.WriteString(fmt.Sprintf("%s: %s\r\n", key, value))
	}
	message.WriteString("\r\n")

	message.WriteString(fmt.Sprintf("--%s\r\n", boundary))

	if content.Body != "" && content.HTMLBody != "" {
		altBoundary := fmt.Sprintf("alternative-%d", time.Now().UnixNano())
		message.WriteString(fmt.Sprintf("Content-Type: multipart/alternative; boundary=%s\r\n\r\n", altBoundary))

		message.WriteString(fmt.Sprintf("--%s\r\n", altBoundary))
		message.WriteString("Content-Type: text/plain; charset=UTF-8\r\n\r\n")
		message.WriteString(content.Body)
		message.WriteString("\r\n\r\n")

		message.WriteString(fmt.Sprintf("--%s\r\n", altBoundary))
		message.WriteString("Content-Type: text/html; charset=UTF-8\r\n\r\n")
		message.WriteString(content.HTMLBody)
		message.WriteString("\r\n\r\n")

		message.WriteString(fmt.Sprintf("--%s--\r\n", altBoundary))
	} else if content.HTMLBody != "" {
		message.WriteString("Content-Type: text/html; charset=UTF-8\r\n\r\n")
		message.WriteString(content.HTMLBody)
		message.WriteString("\r\n\r\n")
	} else {
		message.WriteString("Content-Type: text/plain; charset=UTF-8\r\n\r\n")
		message.WriteString(content.Body)
		message.WriteString("\r\n\r\n")
	}

	for _, att := range content.Attachments {
		message.WriteString(fmt.Sprintf("--%s\r\n", boundary))
		message.WriteString(fmt.Sprintf("Content-Type: %s; name=\"%s\"\r\n", att.ContentType, att.Filename))
		message.WriteString("Content-Transfer-Encoding: base64\r\n")
		message.WriteString(fmt.Sprintf("Content-Disposition: attachment; filename=\"%s\"\r\n\r\n", att.Filename))

		encoded := base64.StdEncoding.EncodeToString(att.Data)

		lineLength := 76
		for i := 0; i < len(encoded); i += lineLength {
			end := i + lineLength
			if end > len(encoded) {
				end = len(encoded)
			}
			message.WriteString(encoded[i:end] + "\r\n")
		}
		message.WriteString("\r\n")
	}

	message.WriteString(fmt.Sprintf("--%s--", boundary))

	auth := smtp.PlainAuth("", s.username, s.password, s.smtpServer)
	addr := fmt.Sprintf("%s:%d", s.smtpServer, s.smtpPort)

	recipients := strings.Split(content.To, ",")
	for i, recipient := range recipients {
		recipients[i] = strings.TrimSpace(recipient)
	}

	err := smtp.SendMail(addr, auth, s.fromEmail, recipients, message.Bytes())
	if err != nil {
		return fmt.Errorf("error sending email: %w", err)
	}

	logger.Info("Sent email",
		"to", content.To,
		"subject", content.Subject,
		"template", content.Template,
		"attachments", len(content.Attachments))

	return nil
}

// CreateMedicalSummaryEmail creates an email with a medical summary
func (s *Sender) CreateMedicalSummaryEmail(to string, patientName string, diagnosisData map[string]any, treatmentData map[string]any) EmailContent {
	templateData := map[string]any{
		"PatientName":   patientName,
		"DiagnosisData": diagnosisData,
		"TreatmentData": treatmentData,
		"AppName":       "SwordSymphony Medical AI",
		"CurrentDate":   time.Now().Format("January 2, 2006"),
		"SupportEmail":  s.fromEmail,
	}

	var textBody strings.Builder
	textBody.WriteString(fmt.Sprintf("Dear %s,\n\n", patientName))
	textBody.WriteString("Thank you for your recent medical consultation. Below is a summary of your diagnosis and treatment plan.\n\n")

	textBody.WriteString("DIAGNOSIS:\n")
	if diagnoses, ok := diagnosisData["potential_diagnoses"].([]any); ok && len(diagnoses) > 0 {
		for i, d := range diagnoses {
			if diagnosis, ok := d.(string); ok {
				textBody.WriteString(fmt.Sprintf("%d. %s\n", i+1, diagnosis))
			}
		}
	}

	textBody.WriteString("\nTREATMENT PLAN:\n")
	if recommendations, ok := treatmentData["recommendations"].([]any); ok && len(recommendations) > 0 {
		for i, r := range recommendations {
			if recommendation, ok := r.(string); ok {
				textBody.WriteString(fmt.Sprintf("%d. %s\n", i+1, recommendation))
			}
		}
	}

	textBody.WriteString("\nMEDICATIONS:\n")
	if medications, ok := treatmentData["medications"].([]any); ok && len(medications) > 0 {
		for i, m := range medications {
			if medication, ok := m.(string); ok {
				textBody.WriteString(fmt.Sprintf("%d. %s\n", i+1, medication))
			}
		}
	}

	textBody.WriteString("\nFOLLOW-UP:\n")
	if followUp, ok := treatmentData["follow_up"].([]any); ok && len(followUp) > 0 {
		for i, f := range followUp {
			if followUpItem, ok := f.(string); ok {
				textBody.WriteString(fmt.Sprintf("%d. %s\n", i+1, followUpItem))
			}
		}
	}

	textBody.WriteString("\nPlease remember that this information is provided for your reference and should be discussed with your healthcare provider.\n\n")
	textBody.WriteString(fmt.Sprintf("If you have any questions, please contact us at %s.\n\n", s.fromEmail))
	textBody.WriteString("Best regards,\nThe SwordSymphony Medical AI Team")

	return EmailContent{
		To:           to,
		Subject:      "Your Medical Consultation Summary",
		Body:         textBody.String(),
		Template:     "medical_summary.html",
		TemplateData: templateData,
	}
}
