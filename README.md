# Daiboon Guard

Daiboon Guard helps moderators of Arabic daily-progress communities apply their posting rules consistently. It checks daily post formatting and image attachment, uses OpenAI to assess relevance and discussion tone, records decisions for moderators, and explains confirmed removals or pending review to authors in Arabic. It is designed for the community where moderators install it; it is not a general-purpose moderation service.

## How it works

- Daily progress posts need a title beginning with `يوم` and a day number, a task description of at least six characters after the separator, and a body of at least 35 characters after links are excluded. The body must mention the same day number and state a meaningful goal. An image attachment is required; image contents are outside the current checks.
- OpenAI classification helps identify off-topic or low-quality posts and harmful discussion. Normal supportive comments and respectful replies are allowed. Moderators can choose live, review, or audit mode in subreddit settings.
- Confirmed removals receive an Arabic explanation with a correction path and Modmail appeal link. Formatting or missing-image removals explain that authors can submit a corrected new post; editing a removed post does not restore it. Filtered items receive a neutral pending-review notice.
- Moderators can open **Daiboon decision** on a post or comment and **Daiboon activity** on the subreddit to inspect explanations, decision details, notification status, and queue health. Every request checks moderator permissions.
- Decision metadata is kept in Reddit-hosted storage for up to 14 days, with a maximum of 1,000 records. Full post and comment bodies and API credentials are not stored in that history. Public Reddit replies and moderator notes follow Reddit's own retention and deletion behavior.

## Data and external service

For classification, the app sends relevant Reddit post/comment text and discussion context to OpenAI using the API key configured by the developer. The app uses the result only to apply the community's moderation rules; it does not use Reddit data to train or fine-tune models. Data sent to OpenAI is subject to OpenAI's API terms and data controls. See the Privacy Policy and Terms linked in the app's Developer Settings for the complete data practices and service terms.

**Fetch domain:** `api.openai.com` — used for text classification of subreddit content. The app does not fetch images for AI review by default.

Arabic policy documents:

- [سياسة الخصوصية](https://github.com/malawadd/daiboon_guard/blob/main/docs/08-%D8%B3%D9%8A%D8%A7%D8%B3%D8%A9-%D8%A7%D9%84%D8%AE%D8%B5%D9%88%D8%B5%D9%8A%D8%A9.md)
- [شروط الاستخدام](https://github.com/malawadd/daiboon_guard/blob/main/docs/09-%D8%B4%D8%B1%D9%88%D8%B7-%D8%A7%D9%84%D8%A7%D8%B3%D8%AA%D8%AE%D8%AF%D8%A7%D9%85.md)

## Install and configure

A moderator with full permissions installs the approved unlisted release in the intended subreddit. In the app's settings, configure the OpenAI API key, choose the moderation mode, and set the daily AI-check limit. Image spot checks default to off. Review the Arabic community rules and post examples in [`docs/00-الفهرس.md`](docs/00-الفهرس.md).

Modes:

- **Live:** remove clear violations and send uncertain cases for moderator review.
- **Review:** send violations to the moderation queue without automatic removals.
- **Audit:** record decisions without ordinary moderation changes. If repeated system failures prevent automated checking, the safety fallback may still send the item to the moderation queue.

The app uses Reddit-native moderator menus and forms; it has no separate dashboard. For an appeal or support request, contact the moderators through the installed community's Modmail.

## Development

Use Node 24 or newer. The development playtest is configured for `r/daiboon_guard_dev`.

```powershell
npm install
npm run test:types
npm run lint
npm run test:unit
npm run build
npm run dev
```

To submit an unlisted release for Reddit review, run `npm run launch`. Every code release must go through Reddit's app review before it can be installed or upgraded in larger communities.
