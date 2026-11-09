# Daiboon Guard: data handling and retention

This document describes the implemented behavior. It is not a completed operator
privacy policy or terms of service for Reddit review.

## Moderation processing

The app reads posts, comments, and discussion context within its installed
subreddit. Valid content may be sent to the configured OpenAI API for classification.
The API key is stored in Devvit developer secrets. Full submission bodies and
credentials are not written into decision history or application logs.

## Decision history and access

Each installation stores at most 1,000 decision records for up to 14 days in its
Reddit-hosted Redis namespace. Records contain content IDs, fingerprints, timestamps,
mode, source/model, sanitized short explanations, confidence, intended action,
confirmed outcome, errors, and reply/note delivery metadata. No submitted post or
comment bodies are retained in these records. Model explanations are sanitized to
remove exact full input echoes and recognizable credential patterns.

History is available through the Daiboon decision and Daiboon activity menus.
Every menu and form request checks the requesting user's moderator permissions
for the current subreddit on the server. Other users do not have access to the
moderator history through these endpoints.

Moderation verdict caches, queue indices, attempt counters and aggregate counters
retain their existing separate TTLs. Aggregate counters contain no content text.

## Public explanations and moderator notes

Confirmed automatic removals receive a public Arabic app reply with the relevant
rule, short explanation, correction guidance, decision reference, and a Modmail
appeal link. Filtered content receives a pending-review explanation. System-failure
fallbacks do not assert a rule violation. Replies do not expose model confidence,
API errors or credentials. Short removal notes are visible to moderators.

These Reddit comments and moderator notes have Reddit's normal lifecycle; the
14-day Redis history TTL does not automatically remove them from Reddit.

## Deletion and delivery

Explicit moderator/admin removal events preserve decision history and notification
jobs while clearing moderation caches. Author deletion events purge the related
history and pending jobs. Unknown deletion sources also trigger conservative
purging. A 14-day tombstone prevents late workers from recreating the purged record.
Event delivery is not guaranteed; TTLs and history pruning provide a fallback.

At most five notification jobs are attempted per minute, with at most four
attempts per job. Replies use an app-author check, stable marker and stored reply
ID to reduce duplicates after ambiguous network failures. This is best-effort
delivery, not an exactly-once guarantee. Ambiguous moderator-note writes may be
retried. Moderator approvals cancel pending deliveries. Exhausted failures are
visible in the moderator activity view until the record expires or is pruned.

The operator must complete and publish their privacy policy and terms, including
contact details and appeal arrangements, and update the app's developer-settings
links before submitting a release to Reddit.
