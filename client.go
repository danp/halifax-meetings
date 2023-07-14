package main

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/PuerkitoBio/goquery"
	"github.com/yosssi/gohtml"
	"golang.org/x/net/html"
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
	for _, tr := range nodes(table.Find("tbody > tr")) {
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

	return MeetingAgenda{ContentHTML: contentHTML}, nil
}

func nodes(s *goquery.Selection) []*goquery.Selection {
	var out []*goquery.Selection
	for _, n := range s.Nodes {
		out = append(out, &goquery.Selection{Nodes: []*html.Node{n}})
	}
	return out
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
