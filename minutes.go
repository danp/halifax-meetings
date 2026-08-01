package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"regexp"
	"strconv"
	"strings"

	"github.com/PuerkitoBio/goquery"
	"golang.org/x/time/rate"
)

type minutesItem struct {
	itemID      int
	number      string
	title       string
	minutesText string
	minutesHTML string
	votes       []minutesVote
}

type minutesVote struct {
	sequence      int
	motionType    string
	result        string
	unanimous     bool
	inFavourCount sql.NullInt64
	againstCount  sql.NullInt64
	rawText       string
	certainty     string
	people        []minutesVotePerson
}

type minutesVotePerson struct {
	name      string
	vote      string
	certainty string
	source    string
}

type minutesContent struct {
	meetingID string
	contentID string
	html      string
}

func processMinutes(ctx context.Context, db *sql.DB, limiter *rate.Limiter, args []string) error {
	const q = `
		select mecu.meeting_id, ecu.external_content_id, ec.html
		from meeting_external_content_urls mecu
		join external_content_urls ecu on ecu.url = mecu.external_content_url
		join external_content ec on ec.id = ecu.external_content_id
		where ecu.url like '%Agenda=PostMinutes%'
			and ec.html is not null
			and ec.html != ''
		order by mecu.meeting_id, ecu.fetched desc, length(ec.html) desc`
	rows, err := db.QueryContext(ctx, q)
	if err != nil {
		return fmt.Errorf("select minutes content: %w", err)
	}
	defer rows.Close()

	var contents []minutesContent
	seenMeetings := make(map[string]struct{})
	for rows.Next() {
		var c minutesContent
		if err := rows.Scan(&c.meetingID, &c.contentID, &c.html); err != nil {
			return fmt.Errorf("scan minutes content: %w", err)
		}
		if _, ok := seenMeetings[c.meetingID]; ok {
			continue
		}
		seenMeetings[c.meetingID] = struct{}{}
		contents = append(contents, c)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("select minutes content: %w", err)
	}
	if err := rows.Close(); err != nil {
		return fmt.Errorf("close minutes content rows: %w", err)
	}

	for _, c := range contents {
		items, err := parseMinutesHTML(c.html)
		if err != nil {
			return fmt.Errorf("parse minutes meeting_id=%v content_id=%v: %w", c.meetingID, c.contentID, err)
		}
		if err := saveMinutes(ctx, db, c.meetingID, c.contentID, items); err != nil {
			return fmt.Errorf("save minutes meeting_id=%v content_id=%v: %w", c.meetingID, c.contentID, err)
		}
	}

	log.Println("processed", len(contents), "minutes")
	return nil
}

func saveMinutes(ctx context.Context, db *sql.DB, meetingID, contentID string, items []minutesItem) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	for _, q := range []string{
		`delete from meeting_minutes_vote_people where meeting_id=?`,
		`delete from meeting_minutes_votes where meeting_id=?`,
		`delete from meeting_minutes_items where meeting_id=?`,
	} {
		if _, err := tx.ExecContext(ctx, q, meetingID); err != nil {
			return fmt.Errorf("delete derived minutes: %w", err)
		}
	}

	for _, item := range items {
		if _, err := tx.ExecContext(ctx, `insert into meeting_minutes_items (meeting_id, external_content_id, item_id, number, title, minutes_text, minutes_html) values (?, ?, ?, ?, ?, ?, ?)`, meetingID, contentID, item.itemID, item.number, item.title, item.minutesText, item.minutesHTML); err != nil {
			return fmt.Errorf("insert minutes item %v: %w", item.itemID, err)
		}

		for _, vote := range item.votes {
			if _, err := tx.ExecContext(ctx, `insert into meeting_minutes_votes (meeting_id, external_content_id, item_id, sequence, motion_type, result, unanimous, in_favour_count, against_count, raw_text, certainty) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`, meetingID, contentID, item.itemID, vote.sequence, vote.motionType, vote.result, boolInt(vote.unanimous), vote.inFavourCount, vote.againstCount, vote.rawText, vote.certainty); err != nil {
				return fmt.Errorf("insert minutes vote item=%v seq=%v: %w", item.itemID, vote.sequence, err)
			}
			for _, person := range vote.people {
				if _, err := tx.ExecContext(ctx, `insert into meeting_minutes_vote_people (meeting_id, external_content_id, item_id, vote_sequence, person_name, vote, certainty, source) values (?, ?, ?, ?, ?, ?, ?, ?) on conflict do nothing`, meetingID, contentID, item.itemID, vote.sequence, person.name, person.vote, person.certainty, person.source); err != nil {
					return fmt.Errorf("insert minutes vote person item=%v seq=%v name=%v: %w", item.itemID, vote.sequence, person.name, err)
				}
			}
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit: %w", err)
	}
	return nil
}

func parseMinutesHTML(html string) ([]minutesItem, error) {
	doc, err := goquery.NewDocumentFromReader(strings.NewReader(html))
	if err != nil {
		return nil, fmt.Errorf("new document: %w", err)
	}

	var items []minutesItem
	for _, s := range doc.Find(".AgendaItem").EachIter() {
		itemID := agendaItemID(s)
		if itemID == 0 {
			continue
		}

		titleRow := s.ChildrenFiltered(".AgendaItemTitleRow").First()
		minutes := s.ChildrenFiltered(".AgendaItemContentRow").First().Find(".AgendaItemMinutes").First()

		var minutesHTML, minutesText string
		if minutes.Length() > 0 {
			var err error
			minutesHTML, err = minutes.Html()
			if err != nil {
				return nil, fmt.Errorf("minutes html item=%v: %w", itemID, err)
			}
			minutesText, err = markdown(minutesHTML)
			if err != nil {
				return nil, fmt.Errorf("minutes markdown item=%v: %w", itemID, err)
			}
			minutesText = strings.TrimSpace(minifyBlankLines(minutesText))
		}

		item := minutesItem{
			itemID:      itemID,
			number:      strings.TrimSpace(titleRow.Find(".AgendaItemCounter").First().Text()),
			title:       strings.TrimSpace(titleRow.Find(".AgendaItemTitle").First().Text()),
			minutesText: minutesText,
			minutesHTML: minutesHTML,
		}
		item.votes = parseMinutesVotes(minutesText)
		items = append(items, item)
	}
	return items, nil
}

func agendaItemID(s *goquery.Selection) int {
	for _, class := range strings.Fields(s.AttrOr("class", "")) {
		if class == "AgendaItem" || !strings.HasPrefix(class, "AgendaItem") {
			continue
		}
		n, err := strconv.Atoi(strings.TrimPrefix(class, "AgendaItem"))
		if err == nil {
			return n
		}
	}
	return 0
}

var voteResultRE = regexp.MustCompile(`(?i)\b(MOTION(?:\s+TO\s+AMEND)?\s+PUT\s+AND\s+(PASSED|DEFEATED)(?:\s+UNANIMOUSLY)?)\.?(?:\s*\((\d+)\s+in\s+favour,\s+(\d+)\s+against\))?`)

func parseMinutesVotes(text string) []minutesVote {
	lines := normalizedLines(text)
	var votes []minutesVote
	for i, line := range lines {
		m := voteResultRE.FindStringSubmatch(line)
		if m == nil {
			continue
		}

		vote := minutesVote{
			sequence:   len(votes) + 1,
			motionType: "main",
			result:     strings.ToLower(m[2]),
			unanimous:  strings.Contains(strings.ToUpper(m[1]), "UNANIMOUSLY"),
			rawText:    voteRawText(lines, i),
			certainty:  "high",
		}
		if strings.Contains(strings.ToUpper(m[1]), "TO AMEND") {
			vote.motionType = "amendment"
		}
		if m[3] != "" {
			vote.inFavourCount = int64Value(m[3])
		}
		if m[4] != "" {
			vote.againstCount = int64Value(m[4])
		}

		sections := voteSections(lines[i+1:])
		for _, name := range sections["in_favour"] {
			vote.people = append(vote.people, minutesVotePerson{name: name, vote: "in_favour", certainty: "high", source: "explicit"})
		}
		for _, name := range sections["against"] {
			vote.people = append(vote.people, minutesVotePerson{name: name, vote: "against", certainty: "high", source: "explicit"})
		}
		for _, name := range sections["not_present"] {
			vote.people = append(vote.people, minutesVotePerson{name: name, vote: "not_present", certainty: "high", source: "explicit"})
		}

		if vote.unanimous && len(sections["in_favour"]) == 0 && len(sections["against"]) == 0 {
			vote.people = append(vote.people, inferredUnanimousPeople(vote.result, sections["not_present"])...)
		}

		votes = append(votes, vote)
	}
	return votes
}

func normalizedLines(text string) []string {
	text = strings.ReplaceAll(text, "\u00a0", " ")
	var lines []string
	for line := range strings.Lines(text) {
		line = strings.ReplaceAll(line, "**", "")
		line = strings.TrimSpace(strings.Trim(line, "*"))
		line = strings.TrimSpace(line)
		if line != "" {
			lines = append(lines, line)
		}
	}
	return lines
}

func voteRawText(lines []string, resultIndex int) string {
	start := resultIndex
	for start > 0 && resultIndex-start < 12 {
		if strings.HasPrefix(strings.ToUpper(lines[start]), "MOVED BY ") {
			break
		}
		start--
	}

	end := resultIndex + 1
	for end < len(lines) && end-resultIndex < 8 {
		upper := strings.ToUpper(lines[end])
		if strings.HasPrefix(lines[end], "#") || strings.HasPrefix(upper, "MOVED BY ") || strings.Contains(upper, "MOTION PUT") || strings.Contains(upper, "MOTION TO AMEND") {
			break
		}
		end++
	}
	return strings.Join(lines[start:end], "\n")
}

func voteSections(lines []string) map[string][]string {
	sections := map[string][]string{}
	for _, line := range lines {
		upper := strings.ToUpper(line)
		if strings.HasPrefix(line, "#") || strings.HasPrefix(upper, "MOVED BY ") || strings.Contains(upper, "MOTION PUT") || strings.Contains(upper, "MOTION TO AMEND") || strings.HasPrefix(upper, "THE QUESTION WAS CALLED") {
			break
		}
		switch {
		case strings.HasPrefix(strings.ToLower(line), "in favour:"):
			sections["in_favour"] = parseVoteNames(strings.TrimSpace(line[len("in favour:"):]))
		case strings.HasPrefix(strings.ToLower(line), "against:"):
			sections["against"] = parseVoteNames(strings.TrimSpace(line[len("against:"):]))
		case strings.HasPrefix(strings.ToLower(line), "not present:"):
			sections["not_present"] = parseVoteNames(strings.TrimSpace(line[len("not present:"):]))
		}
	}
	return sections
}

func inferredUnanimousPeople(result string, notPresent []string) []minutesVotePerson {
	notPresentSet := make(map[string]struct{})
	for _, name := range notPresent {
		notPresentSet[name] = struct{}{}
	}

	vote := "in_favour"
	if result == "defeated" {
		vote = "against"
	}

	var people []minutesVotePerson
	for _, name := range recentCouncilRoster {
		if _, ok := notPresentSet[name]; ok {
			continue
		}
		people = append(people, minutesVotePerson{name: name, vote: vote, certainty: "medium", source: "inferred_unanimous"})
	}
	return people
}

var recentCouncilRoster = []string{
	"Andy Fillmore",
	"Patty Cuttell",
	"Cathy Deagle Gammon",
	"David Hendsbee",
	"Becky Kent",
	"Trish Purdy",
	"Sam Austin",
	"Tony Mancini",
	"Laura White",
	"Virginia Hinch",
	"Shawn Cleary",
	"Kathryn Morse",
	"Janet Steele",
	"Nancy Hartling",
	"John Young",
	"Billy Gillis",
	"Jean St-Amand",
}

var councillorByLastName = map[string]string{
	"fillmore":       "Andy Fillmore",
	"cuttell":        "Patty Cuttell",
	"deagle gammon":  "Cathy Deagle Gammon",
	"hendsbee":       "David Hendsbee",
	"kent":           "Becky Kent",
	"purdy":          "Trish Purdy",
	"austin":         "Sam Austin",
	"mancini":        "Tony Mancini",
	"white":          "Laura White",
	"hinch":          "Virginia Hinch",
	"cleary":         "Shawn Cleary",
	"morse":          "Kathryn Morse",
	"steele":         "Janet Steele",
	"hartling":       "Nancy Hartling",
	"young":          "John Young",
	"gillis":         "Billy Gillis",
	"st-amand":       "Jean St-Amand",
	"st. amand":      "Jean St-Amand",
	"deagle-gammon":  "Cathy Deagle Gammon",
	"deaglegammon":   "Cathy Deagle Gammon",
	"deagle gammon,": "Cathy Deagle Gammon",
}

func parseVoteNames(s string) []string {
	s = strings.ReplaceAll(s, "\u00a0", " ")
	s = strings.Trim(s, " .")
	for _, prefix := range []string{"Budget Committee Chair", "Deputy Mayor", "Councillors", "Councillor", "Mayor"} {
		s = strings.ReplaceAll(s, prefix+" ", "")
	}
	s = strings.ReplaceAll(s, " and ", ", ")

	seen := make(map[string]struct{})
	var names []string
	for _, part := range strings.Split(s, ",") {
		part = strings.Trim(strings.TrimSpace(part), ".")
		if part == "" {
			continue
		}
		name := canonicalCouncillorName(part)
		if _, ok := seen[name]; ok {
			continue
		}
		seen[name] = struct{}{}
		names = append(names, name)
	}
	return names
}

func canonicalCouncillorName(s string) string {
	key := strings.ToLower(strings.TrimSpace(s))
	key = strings.Join(strings.Fields(key), " ")
	if name, ok := councillorByLastName[key]; ok {
		return name
	}
	return strings.TrimSpace(s)
}

func int64Value(s string) sql.NullInt64 {
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return sql.NullInt64{}
	}
	return sql.NullInt64{Int64: n, Valid: true}
}

func boolInt(v bool) int {
	if v {
		return 1
	}
	return 0
}

func minifyBlankLines(s string) string {
	var lines []string
	var previousBlank bool
	for line := range strings.Lines(s) {
		blank := strings.TrimSpace(line) == ""
		if blank && previousBlank {
			continue
		}
		lines = append(lines, strings.TrimRight(line, " \t"))
		previousBlank = blank
	}
	return strings.Join(lines, "")
}
