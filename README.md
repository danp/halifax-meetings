# halifax-meetings

Scraping of
[Halifax Regional Municipality meetings](https://www.halifax.ca/city-hall/agendas-meetings-reports)
to SQLite.

Results viewable at https://hrm.datasette.danp.net/meetings.

## Running it

```
go run .
```

This creates or updates `meetings.db` in the current directory. It runs three steps in order:

* `meetings`: fetches meetings and agendas from halifax.ca and eScribe
* `urls`: fetches documents linked from agendas, plus minutes, and extracts their text
* `minutes`: parses fetched minutes into items and votes

Use `-only` to run some of them, for example `go run . -only meetings` or `go run . -only urls,minutes`.

The `urls` step needs `pdfinfo`, `pdftotext`, `pdftoppm` and `tesseract`. Install `poppler-utils` and `tesseract-ocr` on Ubuntu, or `poppler` and `tesseract` via Homebrew.
