package main

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/base64"
	"fmt"
	"log"
	"net/url"
	"strings"
	"time"
	"unicode"

	"github.com/PuerkitoBio/goquery"
	"golang.org/x/time/rate"
)

func processMeetings(ctx context.Context, db *sql.DB, limiter *rate.Limiter, args []string) error {
	cutoff := time.Now().AddDate(-1, 0, 0)
	var maxObserved time.Time
	if err := db.QueryRow("select max(observed) from meeting_versions").Scan(newTimeValue(&maxObserved)); err != nil {
		return fmt.Errorf("select max meeting_versions observed: %w", err)
	}
	if !maxObserved.IsZero() {
		cutoff = maxObserved.AddDate(0, -8, 0)
	}

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
			return fmt.Errorf("listing meetings: %w", err)
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
		if err := processMeeting(ctx, db, c, m); err != nil {
			return fmt.Errorf("processing meeting date=%v type=%v: %w", m.Event.Date.Format("2006-01-02"), m.Type, err)
		}

		if (i+1)%10 == 0 {
			log.Println("completed", i+1, "/", len(needMeetings), "meetings")
		}
	}

	log.Println("completed", len(needMeetings), "/", len(needMeetings), "meetings")
	return nil
}

func processMeeting(ctx context.Context, db *sql.DB, c Client, m Meeting) error {
	agendaURL := m.URL("agenda")
	if agendaURL == "" {
		return fmt.Errorf("no agenda URL")
	}

	agenda, err := c.Agenda(ctx, agendaURL)
	if err != nil {
		return fmt.Errorf("fetching agenda: %w", err)
	}

	if err := saveMeeting(ctx, db, m, agenda, time.Now()); err != nil {
		return fmt.Errorf("saving: %w", err)
	}
	return nil
}

func saveMeeting(ctx context.Context, db *sql.DB, m Meeting, agenda MeetingAgenda, observed time.Time) error {
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
		return fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	const cq = `insert into meeting_agenda_content (id, text, html) values (?, ?, ?) on conflict (id) do nothing`
	res, err := tx.Exec(cq, contentID, agendaText, agenda.ContentHTML)
	if err != nil {
		return fmt.Errorf("insert meeting agenda content: %w", err)
	}
	ra, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("meeting agenda content rows affected: %w", err)
	}
	if ra > 0 {
		const sq = `insert into meeting_agenda_content_search (rowid, text) values ((select rowid from meeting_agenda_content where id=?), ?)`
		if _, err := tx.Exec(sq, contentID, agendaText); err != nil {
			return fmt.Errorf("insert meeting agenda content search: %w", err)
		}
	}

	const mq = `insert into meetings (id, type, date, schedule_note, agenda_url, minutes_url, video_url, agenda_content_id) values (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8) ON CONFLICT (id) DO UPDATE SET type=excluded.type, date=excluded.date, schedule_note=excluded.schedule_note, agenda_url=excluded.agenda_url, minutes_url=excluded.minutes_url, video_url=excluded.video_url, agenda_content_id=excluded.agenda_content_id`
	if _, err := tx.Exec(mq, id, m.Type, m.Event.Date.Format("2006-01-02"), m.Event.Note, agendaURL, m.URL("minutes"), m.URL("video"), contentID); err != nil {
		return fmt.Errorf("insert meetings: %w", err)
	}

	const vq = `insert into meeting_versions (meeting_id, observed, schedule_note, agenda_url, minutes_url, video_url, agenda_content_id) values (?1, ?2, ?3, ?4, ?5, ?6, ?7) on conflict do nothing`
	if _, err := tx.Exec(vq, id, newTimeValue(&observed), m.Event.Note, agendaURL, m.URL("minutes"), m.URL("video"), contentID); err != nil {
		return fmt.Errorf("insert meeting_versions: %w", err)
	}

	const lq = `update meetings set last_observed=(select max(observed) from meeting_versions where meeting_id=id) where id=?`
	if _, err := tx.Exec(lq, id); err != nil {
		return fmt.Errorf("update meetings last observed: %w", err)
	}

	if err := saveMeetingURLs(ctx, tx, observed, id, agendaURL, contentID, doc); err != nil {
		return fmt.Errorf("saving meeting links: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit: %w", err)
	}
	return nil
}

func saveMeetingURLs(ctx context.Context, tx *sql.Tx, observed time.Time, meetingID, agendaURL, agendaContentID string, doc *goquery.Document) error {
	agendaURLU, err := url.Parse(agendaURL)
	if err != nil {
		return fmt.Errorf("bad agenda URL %v: %w", agendaURL, err)
	}

	for _, a := range nodes(doc.Find("a")) {
		href := abs(agendaURLU, a.AttrOr("href", ""))
		if !strings.HasPrefix(href, "https://www.halifax.ca/media") {
			continue
		}

		if _, err := tx.Exec("insert into external_content_urls (url, added) values (?, ?) on conflict do nothing", href, newTimeValue(&observed)); err != nil {
			return fmt.Errorf("insert external content URL %v: %w", href, err)
		}

		if _, err := tx.Exec("insert into meeting_external_content_urls (meeting_id, agenda_content_id, external_content_url) values (?, ?, ?) on conflict do nothing", meetingID, agendaContentID, href); err != nil {
			return fmt.Errorf("insert meeting external content URL %v: %w", href, err)
		}
	}

	return nil
}
