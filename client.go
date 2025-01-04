package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	htmltomarkdown "github.com/JohannesKaufmann/html-to-markdown/v2"
	"github.com/PuerkitoBio/goquery"
	"github.com/yosssi/gohtml"
)

type MeetingURL struct {
	Name string
	URL  string
}

type MeetingEvent struct {
	Date time.Time
	Note string
}

type Meeting struct {
	ID    string
	Type  string
	Event MeetingEvent
	URLs  []MeetingURL
}

func (m Meeting) URL(name string) string {
	for _, u := range m.URLs {
		if u.Name == name {
			return u.URL
		}
	}
	return ""
}

type MeetingAgenda struct {
	ContentHTML string // should be consistently formatted
	ContentText string // should be consistently formatted
	ContentURLs []string
}

type Client struct {
	Limiter func()
}

func (c Client) List(ctx context.Context, token string) (_ []Meeting, nextToken string, _ error) {
	u := "https://www.halifax.ca/city-hall/agendas-meetings-reports"
	if token != "" {
		u = token
	}
	baseU, err := url.Parse(u)
	if err != nil {
		return nil, "", fmt.Errorf("parsing URL: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, "GET", u, nil)
	if err != nil {
		return nil, "", fmt.Errorf("new request: %w", err)
	}

	if c.Limiter != nil {
		c.Limiter()
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, "", fmt.Errorf("get: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, "", fmt.Errorf("bad status %v", resp.StatusCode)
	}

	doc, err := goquery.NewDocumentFromReader(resp.Body)
	if err != nil {
		return nil, "", fmt.Errorf("new document: %w", err)
	}

	table := doc.Find("table").FilterFunction(func(_ int, s *goquery.Selection) bool {
		id, _ := s.Attr("id")
		return strings.HasPrefix(id, "meetings_listings")
	})
	if table.Length() == 0 {
		return nil, "", fmt.Errorf("url=%v unable to find meetings_listings table", u)
	}

	abs := func(su string) string {
		if su == "" {
			return ""
		}
		rel, err := url.Parse(su)
		if err != nil {
			return ""
		}
		return baseU.ResolveReference(rel).String()
	}

	var meetings []Meeting
	for _, tr := range table.Find("tbody > tr").EachIter() {
		var m Meeting
		var (
			mTime = strings.TrimSpace(tr.Find("td:nth-child(1) time").Text())
			mNote = strings.TrimSpace(tr.Find("td:nth-child(1) strong").Text())
			mType = strings.TrimSpace(tr.Find("td:nth-child(2)").Text())
		)

		const dateFormat = "January 2, 2006"
		mt, err := time.Parse(dateFormat, mTime)
		if err != nil {
			return nil, "", fmt.Errorf("bad meeting date format: %v", mTime)
		}

		m.Type = mType
		m.Event = MeetingEvent{mt, mNote}

		urls := map[string]string{
			"agenda":  abs(tr.Find("td:nth-child(3) a").AttrOr("href", "")),
			"minutes": abs(tr.Find("td:nth-child(4) a").AttrOr("href", "")),
			"video":   abs(tr.Find("td:nth-child(5) a").AttrOr("href", "")),
		}
		for k, s := range urls {
			if s == "" {
				continue
			}
			m.URLs = append(m.URLs, MeetingURL{k, s})
		}

		// sometimes we get things like https://www.halifax.ca/city-hallboards-committees-commissions
		// try and account for that
		id := strings.TrimPrefix(urls["agenda"], "https://www.halifax.ca/city-hall")
		id = strings.TrimPrefix(id, "/")
		id = strings.TrimPrefix(id, "http://legacycontent.halifax.ca/council/")
		m.ID = id

		meetings = append(meetings, m)
	}

	nextLink := doc.Find("#block-views-block-meetings-listings-block-1 li.pager__item.pager__item--next > a")
	nextToken = abs(nextLink.AttrOr("href", ""))

	return meetings, nextToken, nil
}

func (c Client) Agenda(ctx context.Context, agendaURL string) (MeetingAgenda, error) {
	req, err := http.NewRequestWithContext(ctx, "GET", agendaURL, nil)
	if err != nil {
		return MeetingAgenda{}, fmt.Errorf("new request: %w", err)
	}

	if c.Limiter != nil {
		c.Limiter()
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return MeetingAgenda{}, fmt.Errorf("get: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return MeetingAgenda{}, fmt.Errorf("bad status %v", resp.StatusCode)
	}

	doc, err := goquery.NewDocumentFromReader(resp.Body)
	if err != nil {
		return MeetingAgenda{}, fmt.Errorf("new document: %w", err)
	}

	content := doc.Find("#block-halifax-content > div > article > div")
	contentHTML, err := content.Html()
	if err != nil {
		return MeetingAgenda{}, fmt.Errorf("getting content: %w", err)
	}
	contentHTML = gohtml.Format(contentHTML)

	if len(contentHTML) == 0 {
		return MeetingAgenda{}, fmt.Errorf("url=%v did not find content", agendaURL)
	}

	agenda := MeetingAgenda{ContentHTML: contentHTML}

	agendaURLU, err := url.Parse(agendaURL)
	if err != nil {
		return MeetingAgenda{}, fmt.Errorf("bad agenda URL %v: %w", agendaURL, err)
	}

	for _, a := range content.Find("a").EachIter() {
		href := abs(agendaURLU, a.AttrOr("href", ""))
		a.SetAttr("href", href)
		if strings.Contains(href, "halifax.ca/media") || strings.Contains(href, "cdn.halifax.ca") {
			agenda.ContentURLs = append(agenda.ContentURLs, href)
		}
	}

	contentHTML, err = content.Html()
	if err != nil {
		return MeetingAgenda{}, fmt.Errorf("getting content: %w", err)
	}

	md, err := markdown(contentHTML)
	if err != nil {
		return MeetingAgenda{}, fmt.Errorf("converting to markdown: %w", err)
	}
	agenda.ContentText = md

	return agenda, nil
}

type EscribeClient struct {
	Limiter func()
}

func (c EscribeClient) List(ctx context.Context, token string) (_ []Meeting, nextToken string, _ error) {
	if token != "" {
		return nil, "", fmt.Errorf("escribe does not support pagination")
	}

	const u = "https://pub-halifax.escribemeetings.com"
	baseU, err := url.Parse(u)
	if err != nil {
		return nil, "", fmt.Errorf("parsing URL: %w", err)
	}

	abs := func(su string) string {
		if su == "" {
			return ""
		}
		rel, err := url.Parse(su)
		if err != nil {
			return ""
		}
		return baseU.ResolveReference(rel).String()
	}

	now := time.Now()

	var body struct {
		CalendarStartDate time.Time `json:"calendarStartDate"`
		CalendarEndDate   time.Time `json:"calendarEndDate"`
	}
	body.CalendarStartDate = now.AddDate(-1, 0, 0)
	body.CalendarEndDate = now.AddDate(1, 0, 0)

	b, err := json.Marshal(body)
	if err != nil {
		return nil, "", fmt.Errorf("marshal: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u+"/MeetingsCalendarView.aspx/GetAllMeetings", bytes.NewReader(b))
	if err != nil {
		return nil, "", fmt.Errorf("new request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	if c.Limiter != nil {
		c.Limiter()
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, "", fmt.Errorf("get: %w", err)
	}
	defer resp.Body.Close()

	b, err = io.ReadAll(resp.Body)
	if err != nil {
		return nil, "", fmt.Errorf("read body: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, "", fmt.Errorf("bad status %v: %v", resp.StatusCode, string(b))
	}

	var respBody struct {
		D []struct {
			AllowPublicComments     bool   `json:"AllowPublicComments"`
			ClassName               string `json:"ClassName"`
			DelegationRequestLink   string `json:"DelegationRequestLink"`
			Description             string `json:"Description"`
			EndDate                 string `json:"EndDate"`
			FormattedStart          string `json:"FormattedStart"`
			HasAgenda               bool   `json:"HasAgenda"`
			HasLiveVideo            bool   `json:"HasLiveVideo"`
			HasVideo                bool   `json:"HasVideo"`
			HasVideoLivePassed      bool   `json:"HasVideoLivePassed"`
			ID                      string `json:"ID"`
			IsMP3                   bool   `json:"IsMP3"`
			LanguageName            string `json:"LanguageName"`
			LiveVideoStandAloneLink string `json:"LiveVideoStandAloneLink"`
			Location                string `json:"Location"`
			MeetingDocumentLink     []struct {
				AriaLabel          string `json:"AriaLabel"`
				CSSClass           string `json:"CssClass"`
				Format             string `json:"Format"`
				HasLiveVideo       bool   `json:"HasLiveVideo"`
				HasLiveVideoPassed bool   `json:"HasLiveVideoPassed"`
				HasVideo           bool   `json:"HasVideo"`
				HiddenText         string `json:"HiddenText"`
				Image              string `json:"Image"`
				LanguageCode       string `json:"LanguageCode"`
				LanguageID         int    `json:"LanguageId"`
				MeetingName        string `json:"MeetingName"`
				Sequence           any    `json:"Sequence"`
				Title              string `json:"Title"`
				Type               string `json:"Type"`
				URL                string `json:"Url"`
			} `json:"MeetingDocumentLink"`
			MeetingName    string `json:"MeetingName"`
			MeetingPassed  bool   `json:"MeetingPassed"`
			MeetingType    string `json:"MeetingType"`
			PortalID       string `json:"PortalId"`
			ShareURL       string `json:"ShareUrl"`
			Sharing        bool   `json:"Sharing"`
			StartDate      string `json:"StartDate"`
			TimeOverride   string `json:"TimeOverride"`
			TimeOverrideFR string `json:"TimeOverrideFR"`
			URL            string `json:"Url"`
		} `json:"d"`
	}

	if err := json.Unmarshal(b, &respBody); err != nil {
		return nil, "", fmt.Errorf("unmarshal: %w", err)
	}

	var meetings []Meeting
	for _, dm := range respBody.D {
		startDate, _, ok := strings.Cut(dm.StartDate, " ")
		if !ok {
			return nil, "", fmt.Errorf("bad start date %q", dm.StartDate)
		}
		date, err := time.Parse("2006/01/02", startDate)
		if err != nil {
			return nil, "", fmt.Errorf("bad date %q: %w", startDate, err)
		}

		meetingType := dm.MeetingType
		if meetingType == "Halifax Regional Council" {
			meetingType = "Regional Council"
		}

		m := Meeting{
			ID:   dm.ID,
			Type: meetingType,
			Event: MeetingEvent{
				Date: date,
			},
		}
		var hasAgenda bool
		for _, dl := range dm.MeetingDocumentLink {
			if dl.Type == "Agenda" && dl.Format == "HTML" {
				m.URLs = append(m.URLs, MeetingURL{"agenda", abs(dl.URL)})
				hasAgenda = true
				continue
			}
			if dl.Type == "AdditionalDocuments" && dl.Format == ".pdf" && strings.Contains(dl.Title, "Minutes") {
				m.URLs = append(m.URLs, MeetingURL{"minutes", abs(dl.URL)})
				continue
			}
			if dl.Type == "Video" {
				m.URLs = append(m.URLs, MeetingURL{"video", abs(dl.URL)})
				continue
			}
		}

		// Budget Committee - Continuation
		if !hasAgenda && strings.Contains(m.Type, "Continuation") {
			continue
		}

		meetings = append(meetings, m)
	}

	return meetings, "", nil
}

func (c EscribeClient) Agenda(ctx context.Context, agendaURL string) (MeetingAgenda, error) {
	req, err := http.NewRequestWithContext(ctx, "GET", agendaURL, nil)
	if err != nil {
		return MeetingAgenda{}, fmt.Errorf("new request: %w", err)
	}

	if c.Limiter != nil {
		c.Limiter()
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return MeetingAgenda{}, fmt.Errorf("get: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return MeetingAgenda{}, fmt.Errorf("bad status %v", resp.StatusCode)
	}

	doc, err := goquery.NewDocumentFromReader(resp.Body)
	if err != nil {
		return MeetingAgenda{}, fmt.Errorf("new document: %w", err)
	}

	content := doc.Find(".AgendaItems")
	contentHTML, err := content.Html()
	if err != nil {
		return MeetingAgenda{}, fmt.Errorf("getting content: %w", err)
	}
	contentHTML = gohtml.Format(contentHTML)

	if len(contentHTML) == 0 {
		return MeetingAgenda{}, fmt.Errorf("url=%v did not find content", agendaURL)
	}

	// htmltomarkdown below does better with formatted HTML
	doc, err = goquery.NewDocumentFromReader(strings.NewReader(contentHTML))
	if err != nil {
		return MeetingAgenda{}, fmt.Errorf("new document: %w", err)
	}

	content = doc.Selection

	content.Find(".AgendaItemIcons").Remove()

	// <img title="Attachments" src="./_layouts/images/eScribe/attachment.svg" alt="This item has attachments." role="presentation">
	content.Find(`img[title="Attachments"]`).Remove()

	agendaURLU, err := url.Parse(agendaURL)
	if err != nil {
		return MeetingAgenda{}, fmt.Errorf("bad agenda URL %v: %w", agendaURL, err)
	}

	for _, s := range content.Find("a").EachIter() {
		if strings.HasPrefix(s.AttrOr("href", ""), "javascript:") {
			ch, err := s.Html()
			if err != nil {
				return MeetingAgenda{}, fmt.Errorf("getting content: %w", err)
			}
			s.Parent().ReplaceWithHtml(strings.TrimSpace(ch))
			continue
		}
		href := abs(agendaURLU, s.AttrOr("href", ""))
		s.SetAttr("href", href)
	}

	contentHTML, err = content.Html()
	if err != nil {
		return MeetingAgenda{}, fmt.Errorf("getting content: %w", err)
	}
	contentHTML = gohtml.Format(contentHTML)

	md, err := markdown(contentHTML)
	if err != nil {
		return MeetingAgenda{}, fmt.Errorf("converting to markdown: %w", err)
	}

	agenda := MeetingAgenda{ContentHTML: contentHTML, ContentText: md}

	for _, a := range content.Find("a.Link").EachIter() {
		href := abs(agendaURLU, a.AttrOr("href", ""))
		if href == "" {
			continue
		}
		agenda.ContentURLs = append(agenda.ContentURLs, href)
	}

	return agenda, nil
}

func abs(base *url.URL, su string) string {
	if su == "" {
		return ""
	}
	rel, err := url.Parse(su)
	if err != nil {
		return ""
	}
	return base.ResolveReference(rel).String()
}

func markdown(s string) (string, error) {
	md, err := htmltomarkdown.ConvertString(s)
	if err != nil {
		return "", fmt.Errorf("converting to markdown: %w", err)
	}

	var md2 string
	for line := range strings.Lines(md) {
		line = strings.TrimSpace(line)
		md2 += line + "\n"
	}
	return md2, nil
}
