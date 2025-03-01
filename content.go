package main

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/jxskiss/base62"
	"golang.org/x/time/rate"
)

type content struct {
	id    string
	title string
	text  string
}

func processExternalContentURLs(ctx context.Context, db *sql.DB, limiter *rate.Limiter, args []string) error {
	if err := checkPDF(); err != nil {
		return err
	}

	urls, err := unfetchedURLs(ctx, db)
	if err != nil {
		return fmt.Errorf("unfetched urls: %w", err)
	}

	log.Println("need", len(urls), "external content urls")

	start := time.Now()

	for i, u := range urls {
		if err := limiter.Wait(ctx); err != nil {
			return fmt.Errorf("process %v: %w", u, err)
		}
		if err := processURL(ctx, db, u); err != nil {
			return fmt.Errorf("process %v: %w", u, err)
		}

		if (i+1)%10 == 0 {
			log.Println("completed", i+1, "/", len(urls), "external content urls")
		}

		if time.Since(start) > 30*time.Minute {
			log.Println("completed", i+1, "/", len(urls), "external content urls and ran out of time")
			return nil
		}
	}

	log.Println("completed", len(urls), "external content urls")

	return nil
}

func unfetchedURLs(ctx context.Context, db *sql.DB) ([]string, error) {
	rows, err := db.Query("select url from external_content_urls where fetched is null limit 500")
	if err != nil {
		return nil, fmt.Errorf("select: %w", err)
	}
	defer rows.Close()

	var urls []string
	for rows.Next() {
		var u string
		if err := rows.Scan(&u); err != nil {
			return nil, fmt.Errorf("scan: %w", err)
		}
		urls = append(urls, u)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("select: %w", err)
	}
	return urls, nil
}

func processURL(ctx context.Context, db *sql.DB, u string) error {
	now := time.Now()

	saveErr := func(ferr error) error {
		_, err := db.Exec("update external_content_urls set fetched=?, error=? where url=?", newTimeValue(&now), ferr.Error(), u)
		if err != nil {
			return fmt.Errorf("update external_content_urls: %w", err)
		}
		return nil
	}

	uc, ferr := fetchURLContent(ctx, u)
	if ferr != nil {
		if err := saveErr(ferr); err != nil {
			return fmt.Errorf("save error: %w", err)
		}
		return nil
	}
	defer uc.f.Close()
	defer os.Remove(uc.f.Name())

	c := content{id: uc.contentID}

	exists, err := contentExists(ctx, db, c.id)
	if err != nil {
		return fmt.Errorf("checking content ID %v existence: %w", c.id, err)
	}

	if !exists {
		switch uc.contentType {
		case "application/pdf":
			p, perr := processPDF(ctx, uc.f)
			if err != nil {
				if err := saveErr(perr); err != nil {
					return fmt.Errorf("save error: %w", err)
				}
				return nil
			}
			c.title = p.title
			c.text = p.text
		}
	}

	tx, err := db.Begin()
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	if !exists {
		if err := saveContent(ctx, tx, c); err != nil {
			return fmt.Errorf("saving content ID %v: %w", c.id, err)
		}
	}

	var etag sql.NullString
	if uc.etag != "" {
		etag.Valid = true
		etag.String = uc.etag
	}

	if _, err := tx.Exec("update external_content_urls set fetched=?, content_type=?, size=?, last_modified=?, etag=?, error=?, external_content_id=? where url=?", newTimeValue(&now), uc.contentType, uc.size, newTimeValue(&uc.lastModified), etag, nil, c.id, u); err != nil {
		return fmt.Errorf("update external_content_urls: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit: %w", err)
	}
	return nil
}

func contentExists(ctx context.Context, db *sql.DB, id string) (bool, error) {
	var exists bool
	if err := db.QueryRow("select 1 from external_content where id=?", id).Scan(&exists); err != nil && !errors.Is(err, sql.ErrNoRows) {
		return false, fmt.Errorf("select: %w", err)
	}
	return exists, nil
}

func saveContent(ctx context.Context, tx *sql.Tx, c content) error {
	if _, err := tx.Exec("insert into external_content (id, title, text) values (?, ?, ?) on conflict do nothing", c.id, c.title, c.text); err != nil {
		return fmt.Errorf("insert content: %w", err)
	}

	const sq = `insert into external_content_search (rowid, title, text) values ((select rowid from external_content where id=?), ?, ?)`
	if _, err := tx.Exec(sq, c.id, c.title, c.text); err != nil {
		return fmt.Errorf("insert content search: %w", err)
	}
	return nil
}

type urlContent struct {
	f            *os.File
	contentType  string
	contentID    string
	size         int64
	lastModified time.Time
	etag         string
}

func fetchURLContent(ctx context.Context, u string) (_ urlContent, rerr error) {
	ctx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, "GET", u, nil)
	if err != nil {
		return urlContent{}, fmt.Errorf("new request: %w", err)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return urlContent{}, fmt.Errorf("fetch: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return urlContent{}, fmt.Errorf("fetch: bad status %v", resp.StatusCode)
	}

	f, err := os.CreateTemp("", "fetchURLContent")
	if err != nil {
		return urlContent{}, fmt.Errorf("fetch: %w", err)
	}
	defer func() {
		if rerr == nil {
			return
		}
		f.Close()
	}()

	contentSum := sha256.New224()

	size, err := io.Copy(io.MultiWriter(f, contentSum), resp.Body)
	if err != nil {
		return urlContent{}, fmt.Errorf("fetch: %w", err)
	}

	contentID := base62.EncodeToString(contentSum.Sum(nil))

	if _, err := f.Seek(0, 0); err != nil {
		return urlContent{}, fmt.Errorf("fetch: %w", err)
	}

	var lastModified time.Time
	if lm := resp.Header.Get("Last-Modified"); lm != "" {
		t, err := time.Parse(time.RFC1123, lm)
		if err == nil {
			lastModified = t
		}
	}

	return urlContent{f, resp.Header.Get("Content-Type"), contentID, size, lastModified, resp.Header.Get("ETag")}, nil
}

type pdf struct {
	title string
	text  string
}

func checkPDF() error {
	for _, cmd := range []string{"pdfinfo", "pdftotext", "pdftoppm", "tesseract"} {
		_, err := exec.LookPath(cmd)
		if err != nil {
			return fmt.Errorf("missing %v, need to install poppler-utils and tesseract-ocr on ubuntu or poppler and tesseract via homebrew: %w", cmd, err)
		}
	}
	return nil
}

func processPDF(ctx context.Context, f *os.File) (pdf, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()

	tc := exec.CommandContext(ctx, "pdfinfo", f.Name())
	out, err := tc.Output()
	if err != nil {
		return pdf{}, fmt.Errorf("pdfinfo: %w", err)
	}

	var title string
	for l := range strings.SplitSeq(string(out), "\n") {
		if strings.HasPrefix(l, "Title:") {
			title = l
			break
		}
	}

	title = strings.TrimSpace(strings.TrimPrefix(title, "Title:"))
	title = strings.TrimSpace(strings.TrimSuffix(title, "| Halifax.ca"))

	tc = exec.CommandContext(ctx, "pdftotext", f.Name(), "-")
	out, err = tc.Output()
	if err != nil {
		return pdf{}, fmt.Errorf("pdftotext: %w", err)
	}

	if text := strings.TrimSpace(string(out)); text != "" {
		return pdf{title, text}, nil
	}

	td, err := os.MkdirTemp("", "processPDF")
	if err != nil {
		return pdf{}, fmt.Errorf("mkdir temp: %w", err)
	}
	defer os.RemoveAll(td)

	tc = exec.CommandContext(ctx, "pdftoppm", "-png", f.Name(), filepath.Join(td, "page"))
	if err := tc.Run(); err != nil {
		return pdf{}, fmt.Errorf("pdftoppm: %w", err)
	}

	pageFns, err := filepath.Glob(filepath.Join(td, "page*.png"))
	if err != nil {
		return pdf{}, fmt.Errorf("glob: %w", err)
	}

	for _, pageFn := range pageFns {
		if err := exec.CommandContext(ctx, "tesseract", pageFn, pageFn).Run(); err != nil {
			return pdf{}, fmt.Errorf("tesseract: %w", err)
		}
	}

	textFns, err := filepath.Glob(filepath.Join(td, "page*.txt"))
	if err != nil {
		return pdf{}, fmt.Errorf("glob: %w", err)
	}

	var text string
	for _, textFn := range textFns {
		b, err := os.ReadFile(textFn)
		if err != nil {
			return pdf{}, fmt.Errorf("read text: %w", err)
		}
		text += string(b) + "\n"
	}
	return pdf{title, strings.TrimSpace(text)}, nil
}
