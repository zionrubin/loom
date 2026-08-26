package main

import (
	"time"

	"github.com/zionrubin/loom/recall"
)

// The shift: four support conversations, interleaved the way they would arrive
// on a real desk rather than one thread at a time. The companies, the people
// and their problems are fixtures invented for this example.
//
// Two things about the transcript are deliberate. It contains pleasantries, so
// that the Keep stage has something to drop and the report has a number for it;
// and its threads overlap in time, so that the windows the desk cuts are per
// conversation rather than per minute of the feed.
var transcript = []recall.Message{
	line("northwind", "priya", "user", 0, "Hi there, are you around?"),
	line("northwind", "sam", "agent", 30, "Morning Priya, what can I do?"),
	line("northwind", "priya", "user", 55, "We are blocked on SSO — our SAML metadata upload keeps failing with a signature error."),
	line("northwind", "sam", "agent", 90, "Understood. Can you send the assertion so I can check the certificate chain?"),
	line("acme", "dana", "user", 110, "Our nightly export has been failing since the weekend."),
	line("northwind", "priya", "user", 140, "Sent. Also worth saying: our renewal is on the 14th and procurement wants SSO working before they sign."),
	line("acme", "sam", "agent", 165, "Looking now. Which region is the export running in?"),
	line("northwind", "sam", "agent", 200, "Thanks — the chain is fine, it is our validator being strict. We will ship a fix for SAML signature handling this week."),
	line("acme", "dana", "user", 220, "eu-west. It has failed four nights running, always at the same step."),
	line("meridian", "lee", "user", 240, "Can you resend the invoice for March? Finance never received it."),
	line("acme", "sam", "agent", 275, "That step writes to the archive bucket. There is an outage on that storage tier in eu-west; I am escalating it to the platform team."),
	line("meridian", "sam", "agent", 300, "Sure thing, resending now."),
	line("northwind", "priya", "user", 330, "That works for us. Thank you."),
	line("acme", "dana", "user", 360, "Please do. We need the export by Friday or our finance close slips."),
	line("orbit", "kai", "user", 380, "We prefer email over the in-app notifications — can that be changed per user?"),
	line("acme", "sam", "agent", 410, "Escalated. The platform team expects the storage tier back within the day, and we will backfill the missed exports overnight."),
	line("orbit", "sam", "agent", 445, "Not per user today, only per workspace. I will log it as a request."),
	line("meridian", "lee", "user", 470, "Got it, thanks. One more: our renewal quote still shows last year's seat count."),
	line("orbit", "kai", "user", 500, "Understood. Per workspace is fine for now."),
	line("meridian", "sam", "agent", 530, "I will get the quote corrected — we will have an updated one to you tomorrow."),
	line("acme", "dana", "user", 560, "Any update on the storage tier? Our close is Friday."),
	line("northwind", "priya", "user", 590, "One more thing — is the SAML fix going to need a maintenance window on our side?"),
	line("acme", "sam", "agent", 620, "The tier is back. The backfill is running and the export should be green tomorrow morning."),
	line("northwind", "sam", "agent", 650, "No window needed, it is server side only."),
}

// line builds one message at an offset in seconds from the start of the shift.
func line(conversation, speaker, role string, offset int, text string) recall.Message {
	return recall.Message{
		Conversation: conversation,
		Speaker:      speaker,
		Role:         role,
		Text:         text,
		// At is filled in by shift, relative to when the example runs: a Feed
		// is a live source and closes its windows against the present, so a
		// transcript stamped with a fixed date would arrive already late.
		At: time.Unix(int64(offset), 0),
	}
}

// shift stamps the transcript onto wall clock and gives each message a stable
// ID, so a redelivery is recognizable rather than a second conversation.
//
// It is called with an end time rather than assuming one, because when the
// shift finished decides how long the desk takes to become queryable. A window
// closes when event time passes its end, so a transcript ending now leaves its
// last slice open for up to a window; a transcript that ended a window ago is
// complete the moment the feed goes quiet. The backfill uses the second, and
// live delivery — where messages are stamped as they are posted — has no
// choice but the first.
func shift(end time.Time, msgs []recall.Message) []recall.Message {
	if len(msgs) == 0 {
		return nil
	}
	base := msgs[0].At
	span := msgs[len(msgs)-1].At.Sub(base)
	start := end.Add(-span)

	out := make([]recall.Message, 0, len(msgs))
	for i, m := range msgs {
		m.ID = m.Conversation + "-" + itoa(i)
		m.At = start.Add(m.At.Sub(base)).UTC()
		out = append(out, m)
	}
	return out
}

// compress rescales the transcript's event-time span onto d, so an eleven
// minute shift can be delivered as a few seconds of live traffic.
func compress(msgs []recall.Message, d time.Duration) []recall.Message {
	if len(msgs) < 2 || d <= 0 {
		return msgs
	}
	base := msgs[0].At
	span := msgs[len(msgs)-1].At.Sub(base)
	if span <= 0 {
		return msgs
	}
	out := make([]recall.Message, 0, len(msgs))
	for _, m := range msgs {
		m.At = base.Add(time.Duration(float64(m.At.Sub(base)) / float64(span) * float64(d)))
		out = append(out, m)
	}
	return out
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var b []byte
	for n > 0 {
		b = append([]byte{byte('0' + n%10)}, b...)
		n /= 10
	}
	return string(b)
}
