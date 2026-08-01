package main

import "testing"

func TestParseMinutesVotesExplicitNamedVote(t *testing.T) {
	text := `MOVED by Councillor Austin, seconded by Councillor White

**THAT Halifax Regional Council direct the Chief Administrative Officer to prepare a staff report.**

**MOTION PUT AND PASSED.** (15 in favour, 1 against)

In favour: Mayor Fillmore, Deputy Mayor Cuttell, Councillors Hendsbee, Kent, Austin, Mancini, White, Hinch, Cleary, Morse, Steele, Hartling, Young, Gillis, St-Amand

Against: Councillor Purdy

Not present: Councillor Deagle Gammon`

	votes := parseMinutesVotes(text)
	if len(votes) != 1 {
		t.Fatalf("len(votes) = %d, want 1", len(votes))
	}
	vote := votes[0]
	if vote.motionType != "main" || vote.result != "passed" || vote.unanimous {
		t.Fatalf("vote = %+v", vote)
	}
	if !vote.inFavourCount.Valid || vote.inFavourCount.Int64 != 15 {
		t.Fatalf("in favour count = %+v, want 15", vote.inFavourCount)
	}
	if !vote.againstCount.Valid || vote.againstCount.Int64 != 1 {
		t.Fatalf("against count = %+v, want 1", vote.againstCount)
	}

	assertPersonVote(t, vote.people, "Andy Fillmore", "in_favour", "explicit")
	assertPersonVote(t, vote.people, "Trish Purdy", "against", "explicit")
	assertPersonVote(t, vote.people, "Cathy Deagle Gammon", "not_present", "explicit")
}

func TestParseMinutesVotesInfersUnanimousVote(t *testing.T) {
	text := `MOVED by Councillor Kent, seconded by Councillor Hendsbee

**THAT Halifax Regional Council schedule a public hearing.**

**MOTION PUT AND PASSED UNANIMOUSLY.**

Not present: Councillors Purdy and White`

	votes := parseMinutesVotes(text)
	if len(votes) != 1 {
		t.Fatalf("len(votes) = %d, want 1", len(votes))
	}
	vote := votes[0]
	if !vote.unanimous || vote.result != "passed" {
		t.Fatalf("vote = %+v", vote)
	}

	assertPersonVote(t, vote.people, "Becky Kent", "in_favour", "inferred_unanimous")
	assertPersonVote(t, vote.people, "Trish Purdy", "not_present", "explicit")
	assertNoPersonVote(t, vote.people, "Trish Purdy", "in_favour")
	assertNoPersonVote(t, vote.people, "Laura White", "in_favour")
}

func assertPersonVote(t *testing.T, people []minutesVotePerson, name, vote, source string) {
	t.Helper()
	for _, person := range people {
		if person.name == name && person.vote == vote && person.source == source {
			return
		}
	}
	t.Fatalf("missing person vote name=%q vote=%q source=%q in %+v", name, vote, source, people)
}

func assertNoPersonVote(t *testing.T, people []minutesVotePerson, name, vote string) {
	t.Helper()
	for _, person := range people {
		if person.name == name && person.vote == vote {
			t.Fatalf("unexpected person vote name=%q vote=%q in %+v", name, vote, people)
		}
	}
}
