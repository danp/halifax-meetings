package main

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"flag"
	"fmt"
	"log"
	"os"
	"sort"
	"strings"
	"time"

	_ "github.com/ncruces/go-sqlite3/driver"
	_ "github.com/ncruces/go-sqlite3/embed"
	"golang.org/x/time/rate"
)

func main() {
	ctx := context.Background()

	db, err := sql.Open("sqlite3", "file:meetings.db?_pragma=busy_timeout(5000)")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	if err := initDB(db); err != nil {
		log.Fatal(err)
	}

	limiter := rate.NewLimiter(rate.Every(time.Second), 1)

	fs := flag.NewFlagSet("halifax-meetings", flag.ExitOnError)
	var only commaSeparatedString
	fs.Var(&only, "only", "only run these comma-separated actions")
	fs.Parse(os.Args[1:])

	type action struct {
		name string
		fn   func(_ context.Context, _ *sql.DB, _ *rate.Limiter, args []string) error
	}
	actions := []action{
		{"meetings", processMeetings},
		{"urls", processExternalContentURLs},
	}
	for _, a := range actions {
		if len(only.vals) > 0 {
			if _, ok := only.vals[a.name]; !ok {
				continue
			}
		}

		if err := a.fn(ctx, db, limiter, fs.Args()); err != nil {
			log.Fatal(err)
		}
	}
}

func initDB(db *sql.DB) error {
	initQueries := []string{
		`create table if not exists meeting_agenda_content (id text primary key, text text, html text)`,
		`create table if not exists meetings (id text primary key, type text, date text, schedule_note text, last_observed datetime, updated datetime, agenda_url text, minutes_url text, video_url text, agenda_content_id references meeting_agenda_content (id))`,
		`create table if not exists meeting_versions (meeting_id text references meetings (id), observed datetime, schedule_note text, agenda_url text, minutes_url text, video_url text, agenda_content_id references meeting_agenda_content (id), unique (meeting_id, schedule_note, agenda_url, minutes_url, video_url, agenda_content_id))`,
		`create index if not exists meetings_agenda_content_id on meetings (agenda_content_id)`,
		`create virtual table if not exists meeting_agenda_content_search using fts5(text, content=meeting_agenda_content)`,
		`create table if not exists external_content (id text primary key, title text, text text)`,
		`create virtual table if not exists external_content_search using fts5(title, text, content=external_content)`,
		`create table if not exists external_content_urls (url text primary key, added datetime, fetched datetime, content_type text, size integer, last_modified datetime, etag text, error text, external_content_id text references external_content (id))`,
		`create table if not exists meeting_external_content_urls (meeting_id text references meetings (id), agenda_content_id references meeting_agenda_content (id), external_content_url text references external_content_urls (url), unique (meeting_id, agenda_content_id, external_content_url))`,
		`create index if not exists external_content_urls_external_content_id on external_content_urls (external_content_id)`,
		`create index if not exists meeting_external_content_urls_external_content_url on meeting_external_content_urls (external_content_url)`,
	}
	for _, q := range initQueries {
		if _, err := db.Exec(q); err != nil {
			return fmt.Errorf("init db: %w", err)
		}
	}
	return nil
}

const timeFormat = "2006-01-02 15:04:05.999"

type timeValue struct {
	v *time.Time
}

func newTimeValue(v *time.Time) timeValue {
	return timeValue{v: v}
}

func (s timeValue) Scan(src any) error {
	if src == nil {
		*s.v = time.Time{}
		return nil
	}

	if vt, ok := src.(time.Time); ok {
		*s.v = vt.UTC()
		return nil
	}

	vs, ok := src.(string)
	if !ok {
		return fmt.Errorf("unknown timeValue.Scan type %T", src)
	}

	t, err := time.ParseInLocation(timeFormat, vs, time.UTC)
	if err == nil {
		*s.v = t
		return nil
	}

	return fmt.Errorf("could not parse time string %q", vs)
}

func (s timeValue) Value() (driver.Value, error) {
	if s.v.IsZero() {
		return nil, nil
	}
	return s.v.UTC().Format(timeFormat), nil
}

type commaSeparatedString struct {
	vals map[string]struct{}
}

func (c *commaSeparatedString) Set(s string) error {
	c.vals = make(map[string]struct{})
	for s := range strings.SplitSeq(s, ",") {
		c.vals[s] = struct{}{}
	}
	return nil
}

func (c *commaSeparatedString) String() string {
	var keys []string
	for k := range c.vals {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return strings.Join(keys, ",")
}
