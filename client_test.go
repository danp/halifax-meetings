package main

import "testing"

func TestEscribeMinutesURLPrefersPostMinutesPDF(t *testing.T) {
	links := []EscribeMeetingDocumentLink{
		{Type: "AdditionalDocuments", Format: ".pdf", Title: "Draft Minutes - July 14, 2026 Regional Council", URL: "/FileStream.ashx?DocumentId=5686"},
		{Type: "PostMinutes", Format: ".pdf", Title: "Minutes (PDF)", URL: "/FileStream.ashx?DocumentId=5650"},
		{Type: "PostMinutes", Format: "HTML", Title: "Minutes (HTML)", URL: "/Meeting.aspx?Agenda=PostMinutes"},
	}

	got := escribeMinutesURL(links, func(s string) string { return "https://pub-halifax.escribemeetings.com" + s })
	want := "https://pub-halifax.escribemeetings.com/FileStream.ashx?DocumentId=5650"
	if got != want {
		t.Fatalf("minutes URL = %q, want %q", got, want)
	}
}

func TestEscribePostMinutesHTMLURL(t *testing.T) {
	links := []EscribeMeetingDocumentLink{
		{Type: "PostMinutes", Format: ".pdf", Title: "Minutes (PDF)", URL: "/FileStream.ashx?DocumentId=5650"},
		{Type: "PostMinutes", Format: "HTML", Title: "Minutes (HTML)", URL: "/Meeting.aspx?Agenda=PostMinutes"},
	}

	got := escribePostMinutesHTMLURL(links, func(s string) string { return "https://pub-halifax.escribemeetings.com" + s })
	want := "https://pub-halifax.escribemeetings.com/Meeting.aspx?Agenda=PostMinutes"
	if got != want {
		t.Fatalf("minutes HTML URL = %q, want %q", got, want)
	}
}

func TestEscribeMinutesHTMLURLForAgendaURL(t *testing.T) {
	got := escribeMinutesHTMLURLForAgendaURL("https://pub-halifax.escribemeetings.com/Meeting.aspx?Id=7ec924d6-96b8-4dec-b65a-d1d6ce3e43fa&Agenda=Agenda&lang=English")
	want := "https://pub-halifax.escribemeetings.com/Meeting.aspx?Agenda=PostMinutes&Id=7ec924d6-96b8-4dec-b65a-d1d6ce3e43fa&lang=English"
	if got != want {
		t.Fatalf("minutes HTML URL = %q, want %q", got, want)
	}
}

func TestEscribeMinutesURLFallsBackToDraftAdditionalDocumentPDF(t *testing.T) {
	links := []EscribeMeetingDocumentLink{
		{Type: "Agenda", Format: "HTML", Title: "Agenda (HTML)", URL: "/Meeting.aspx?Agenda=Agenda"},
		{Type: "AdditionalDocuments", Format: ".pdf", Title: "Draft Minutes - July 14, 2026 Regional Council", URL: "/FileStream.ashx?DocumentId=5686"},
	}

	got := escribeMinutesURL(links, func(s string) string { return "https://pub-halifax.escribemeetings.com" + s })
	want := "https://pub-halifax.escribemeetings.com/FileStream.ashx?DocumentId=5686"
	if got != want {
		t.Fatalf("minutes URL = %q, want %q", got, want)
	}
}
