package main

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/base64"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"strings"
	"time"
	"unicode"

	"github.com/PuerkitoBio/goquery"
	"github.com/yosssi/gohtml"
	"golang.org/x/net/html"
	"golang.org/x/time/rate"
	_ "modernc.org/sqlite"
)

func main() {
	ctx := context.Background()

	db, err := sql.Open("sqlite", "meetings.db?_pragma=foreign_keys(1)")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	if err := initDB(db); err != nil {
		log.Fatal(err)
	}

	cutoff := time.Now().AddDate(-1, 0, 0)
	var maxObserved sql.NullString
	if err := db.QueryRow("select max(observed) from meeting_versions").Scan(&maxObserved); err != nil {
		log.Fatal(err)
	}
	if maxObserved.Valid {
		mo, err := time.Parse("2006-01-02 15:04:05.999", maxObserved.String)
		if err != nil {
			log.Fatal(err)
		}
		cutoff = mo.AddDate(0, -8, 0)
	}

	limiter := rate.NewLimiter(rate.Every(time.Second), 1)

	var c Client
	c.Limiter = func() {
		if err := limiter.Wait(ctx); err != nil {
			log.Println(err)
		}
	}

	var needMeetings []Meeting
	var token string
outer:
	for {
		meetings, nextToken, err := c.List(ctx, token)
		if err != nil {
			log.Fatal(err)
		}

		for _, m := range meetings {
			if m.Event.Date.Before(cutoff) {
				break outer
			}

			needMeetings = append(needMeetings, m)
		}

		if nextToken == "" {
			break
		}
		token = nextToken
	}

	// TODO: weed out ones we can consider done, such as have non-draft minutes
	log.Println("need", len(needMeetings), "meetings >=", cutoff.Format(time.RFC3339))

	for i, m := range needMeetings {
		if err := process(ctx, db, c, m); err != nil {
			log.Fatal(fmt.Errorf("processing date=%v type=%v: %w", m.Event.Date.Format("2006-01-02"), m.Type, err))
		}

		if (i+1)%10 == 0 {
			log.Println("completed", i+1, "/", len(needMeetings))
		}
	}

	log.Println("completed", len(needMeetings), "/", len(needMeetings))
}

func initDB(db *sql.DB) error {
	initQueries := []string{
		`create table if not exists meeting_agenda_content (id text primary key, text text, html text)`,
		`create table if not exists meetings (id text primary key, type text, date text, schedule_note text, last_observed datetime, agenda_url text, minutes_url text, video_url text, agenda_content_id references meeting_agenda_content (id))`,
		`create table if not exists meeting_versions (meeting_id text references meetings (id), observed datetime, schedule_note text, agenda_url text, minutes_url text, video_url text, agenda_content_id references meeting_agenda_content (id), unique (meeting_id, schedule_note, agenda_url, minutes_url, video_url, agenda_content_id))`,
		`create index if not exists meetings_agenda_content_id on meetings (agenda_content_id)`,
		`create virtual table if not exists meeting_agenda_content_search using fts5(text, content=meeting_agenda_content)`,
	}
	for _, q := range initQueries {
		if _, err := db.Exec(q); err != nil {
			return fmt.Errorf("init db: %w", err)
		}
	}
	return nil
}

func process(ctx context.Context, db *sql.DB, c Client, m Meeting) error {
	agendaURL := m.URL("agenda")
	if agendaURL == "" {
		return fmt.Errorf("no agenda URL")
	}

	agenda, err := c.Agenda(ctx, agendaURL)
	if err != nil {
		return fmt.Errorf("fetching agenda: %w", err)
	}

	if err := save(ctx, db, m, agenda, time.Now()); err != nil {
		return fmt.Errorf("saving: %w", err)
	}
	return nil
}

func save(ctx context.Context, db *sql.DB, m Meeting, agenda MeetingAgenda, observed time.Time) error {
	doc, err := goquery.NewDocumentFromReader(strings.NewReader(agenda.ContentHTML))
	if err != nil {
		return err
	}

	agendaLines := strings.Split(strings.TrimSpace(doc.Text()), "\n")
	var agendaText string
	for _, l := range agendaLines {
		l = strings.TrimRightFunc(l, unicode.IsSpace)
		if l == "" && strings.HasSuffix(agendaText, "\n\n") {
			continue
		}
		agendaText += l + "\n"
	}

	contentSum := sha256.New224()
	fmt.Fprintln(contentSum, agenda.ContentHTML)
	contentID := base64.RawURLEncoding.EncodeToString(contentSum.Sum(nil))

	agendaURL := m.URL("agenda")
	if agendaURL == "" {
		return fmt.Errorf("no agenda URL")
	}
	// sometimes we get things like https://www.halifax.ca/city-hallboards-committees-commissions
	// try and account for that
	id := strings.TrimPrefix(agendaURL, "https://www.halifax.ca/city-hall")
	id = strings.TrimPrefix(id, "/")

	id = strings.TrimPrefix(id, "http://legacycontent.halifax.ca/council/")

	tx, err := db.Begin()
	if err != nil {
		return fmt.Errorf("begin: %w", err)
	}
	defer tx.Rollback()

	const cq = `insert into meeting_agenda_content (id, text, html) values (?, ?, ?) on conflict (id) do nothing`
	res, err := tx.Exec(cq, contentID, agendaText, agenda.ContentHTML)
	if err != nil {
		return fmt.Errorf("meeting_agenda content insert: %w", err)
	}
	ra, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("meeting_agenda content insert rows affected: %w", err)
	}
	if ra > 0 {
		const sq = `insert into meeting_agenda_content_search (rowid, text) values ((select rowid from meeting_agenda_content where id=?), ?)`
		if _, err := tx.Exec(sq, contentID, agendaText); err != nil {
			return fmt.Errorf("meeting_agenda_content_search insert: %w", err)
		}
	}

	const mq = `insert into meetings (id, type, date, schedule_note, agenda_url, minutes_url, video_url, agenda_content_id) values (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8) ON CONFLICT (id) DO UPDATE SET type=excluded.type, date=excluded.date, schedule_note=excluded.schedule_note, agenda_url=excluded.agenda_url, minutes_url=excluded.minutes_url, video_url=excluded.video_url, agenda_content_id=excluded.agenda_content_id`
	if _, err := tx.Exec(mq, id, m.Type, m.Event.Date.Format("2006-01-02"), m.Event.Note, agendaURL, m.URL("minutes"), m.URL("video"), contentID); err != nil {
		return fmt.Errorf("meetings insert: %w", err)
	}

	observedS := observed.UTC().Format("2006-01-02 15:04:05.999")

	const vq = `insert into meeting_versions (meeting_id, observed, schedule_note, agenda_url, minutes_url, video_url, agenda_content_id) values (?1, ?2, ?3, ?4, ?5, ?6, ?7) on conflict do nothing`
	if _, err := tx.Exec(vq, id, observedS, m.Event.Note, agendaURL, m.URL("minutes"), m.URL("video"), contentID); err != nil {
		return fmt.Errorf("meeting_versions insert: %w", err)
	}

	const lq = `update meetings set last_observed=(select max(observed) from meeting_versions where meeting_id=id) where id=?`
	if _, err := tx.Exec(lq, id); err != nil {
		return fmt.Errorf("meetings last_observed update: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit: %w", err)
	}
	return nil
}

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

type MeetingAgenda struct {
	ContentHTML string // should be consistently formatted
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
