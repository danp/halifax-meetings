package main

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"math/rand/v2"
	"time"

	"github.com/jxskiss/base62"
	"golang.org/x/time/rate"
)

func processMeetings(ctx context.Context, db *sql.DB, limiter *rate.Limiter, args []string) error {
	cutoff := time.Now().AddDate(0, -1, 0)
	var maxObserved time.Time
	if err := db.QueryRow("select max(last_observed) from meetings").Scan(newTimeValue(&maxObserved)); err != nil {
		return fmt.Errorf("select max meeting_versions observed: %w", err)
	}
	if !maxObserved.IsZero() {
		cutoff = maxObserved.AddDate(0, -8, 0)
	}

	waitLimiter := func() {
		if err := limiter.Wait(ctx); err != nil {
			log.Println(err)
		}
	}

	var (
		halifaxClient = Client{Limiter: waitLimiter}
		escribeClient = EscribeClient{Limiter: waitLimiter}
	)

	type client interface {
		List(context.Context, string) ([]Meeting, string, error)
		agendaer
	}

	type meetingAgendaer struct {
		m Meeting
		a agendaer
	}
	var needMeetings []meetingAgendaer
	for _, c := range []client{halifaxClient, escribeClient} {
		err := func() error {
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
					if fresh, err := isMeetingFresh(ctx, db, m); err != nil {
						return fmt.Errorf("checking freshness: %w", err)
					} else if fresh {
						continue
					}
					needMeetings = append(needMeetings, meetingAgendaer{m, c})
				}

				if nextToken == "" {
					break
				}
				token = nextToken
			}

			return nil
		}()
		if err != nil {
			return fmt.Errorf("listing meetings: %w", err)
		}
	}

	log.Println("need", len(needMeetings), "meetings >=", cutoff.Format(time.RFC3339))

	for i, ma := range needMeetings {
		if err := processMeeting(ctx, db, ma.a, ma.m); err != nil {
			return fmt.Errorf("processing meeting date=%v type=%v: %w", ma.m.Event.Date.Format("2006-01-02"), ma.m.Type, err)
		}

		if (i+1)%10 == 0 {
			log.Println("completed", i+1, "/", len(needMeetings), "meetings")
		}
	}

	log.Println("completed", len(needMeetings), "/", len(needMeetings), "meetings")
	return nil
}

func isMeetingFresh(ctx context.Context, db *sql.DB, m Meeting) (bool, error) {
	var lastObserved time.Time
	if err := db.QueryRowContext(ctx, "select last_observed from meetings where id=?", m.ID).Scan(newTimeValue(&lastObserved)); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return false, nil
		}
		return false, fmt.Errorf("select last observed: %w", err)
	}

	now := time.Now()

	threshold := time.Hour - 2*time.Minute // to account for hourly run jitter
	if m.Event.Date.Before(now.AddDate(0, 0, -7)) {
		const jitter = 2 * time.Hour
		threshold = 24*time.Hour + (rand.N(jitter) - jitter/2)
	}

	return now.Sub(lastObserved) < threshold, nil
}

type agendaer interface {
	Agenda(context.Context, string) (MeetingAgenda, error)
}

func processMeeting(ctx context.Context, db *sql.DB, a agendaer, m Meeting) error {
	agendaURL := m.URL("agenda")
	if agendaURL == "" {
		return fmt.Errorf("no agenda URL")
	}

	agenda, err := a.Agenda(ctx, agendaURL)
	if err != nil {
		return fmt.Errorf("fetching agenda: %w", err)
	}

	if err := saveMeeting(db, m, agenda, time.Now()); err != nil {
		return fmt.Errorf("saving: %w", err)
	}
	return nil
}

func saveMeeting(db *sql.DB, m Meeting, agenda MeetingAgenda, observed time.Time) error {
	contentSum := sha256.New224()
	fmt.Fprintln(contentSum, agenda.ContentHTML)
	contentID := base62.EncodeToString(contentSum.Sum(nil))

	agendaURL := m.URL("agenda")
	if agendaURL == "" {
		return fmt.Errorf("no agenda URL")
	}

	tx, err := db.Begin()
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	const cq = `insert into meeting_agenda_content (id, text, html) values (?, ?, ?) on conflict (id) do nothing`
	res, err := tx.Exec(cq, contentID, agenda.ContentText, agenda.ContentHTML)
	if err != nil {
		return fmt.Errorf("insert meeting agenda content: %w", err)
	}
	ra, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("meeting agenda content rows affected: %w", err)
	}
	if ra > 0 {
		const sq = `insert into meeting_agenda_content_search (rowid, text) values ((select rowid from meeting_agenda_content where id=?), ?)`
		if _, err := tx.Exec(sq, contentID, agenda.ContentText); err != nil {
			return fmt.Errorf("insert meeting agenda content search: %w", err)
		}
	}

	const mq = `insert into meetings (id, type, date, schedule_note, agenda_url, minutes_url, video_url, agenda_content_id) values (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8) ON CONFLICT (id) DO UPDATE SET type=excluded.type, date=excluded.date, schedule_note=excluded.schedule_note, agenda_url=excluded.agenda_url, minutes_url=excluded.minutes_url, video_url=excluded.video_url, agenda_content_id=excluded.agenda_content_id`
	if _, err := tx.Exec(mq, m.ID, m.Type, m.Event.Date.Format("2006-01-02"), m.Event.Note, agendaURL, m.URL("minutes"), m.URL("video"), contentID); err != nil {
		return fmt.Errorf("insert meetings: %w", err)
	}

	const vq = `insert into meeting_versions (meeting_id, observed, schedule_note, agenda_url, minutes_url, video_url, agenda_content_id) values (?1, ?2, ?3, ?4, ?5, ?6, ?7) on conflict do nothing`
	if _, err := tx.Exec(vq, m.ID, newTimeValue(&observed), m.Event.Note, agendaURL, m.URL("minutes"), m.URL("video"), contentID); err != nil {
		return fmt.Errorf("insert meeting_versions: %w", err)
	}

	const lq = `update meetings set updated=(select max(observed) from meeting_versions where meeting_id=id), last_observed=? where id=?`
	if _, err := tx.Exec(lq, newTimeValue(&observed), m.ID); err != nil {
		return fmt.Errorf("update meetings last observed: %w", err)
	}

	if err := saveMeetingURLs(tx, observed, m.ID, contentID, agenda); err != nil {
		return fmt.Errorf("saving meeting links: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit: %w", err)
	}
	return nil
}

func saveMeetingURLs(tx *sql.Tx, observed time.Time, meetingID, agendaContentID string, agenda MeetingAgenda) error {
	for _, u := range agenda.ContentURLs {
		if _, err := tx.Exec("insert into external_content_urls (url, added) values (?, ?) on conflict do nothing", u, newTimeValue(&observed)); err != nil {
			return fmt.Errorf("insert external content URL %v: %w", u, err)
		}

		if _, err := tx.Exec("insert into meeting_external_content_urls (meeting_id, agenda_content_id, external_content_url) values (?, ?, ?) on conflict do nothing", meetingID, agendaContentID, u); err != nil {
			return fmt.Errorf("insert meeting external content URL %v: %w", u, err)
		}
	}

	return nil
}
