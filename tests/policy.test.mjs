import assert from 'node:assert/strict';
import test from 'node:test';
import { normalizeDigits, redditImageUrl, extractImageUrls, checkPostFormat, commentTarget, enforcement, shouldSampleImage } from '../src/server/policy.ts';
const image = 'https://i.redd.it/abc123.png';
test('Gallery images accept SDK numeric VALID status and exclude failed uploads', () => {
  assert.deepEqual(extractImageUrls({gallery:[
    {url:image,status:1},
    {url:'https://i.redd.it/failed.png',status:2},
    {url:'https://i.redd.it/legacy.png',status:'VALID'},
    {url:'https://i.redd.it/failed-legacy.png',status:'FAILED'},
  ]}), [image,'https://i.redd.it/legacy.png']);
});
const body = 'اليوم ١: أنوي تصميم واجهة متجر إلكتروني وتجربة نموذج أولي لصفحة المنتجات.';

test('Arabic and Persian digits normalize', () => {
  assert.equal(normalizeDigits('يوم ١ / ۲ / 3'), 'يوم 1 / 2 / 3');
});
test('Correct day, title, goal and image passes deterministic format', () => {
  assert.deepEqual(checkPostFormat('يوم ١، تصميم واجهة متجر', body, [image]), { ok: true, day: 1 });
});
test('Unrelated title format rejects instantly', () => {
  assert.equal(checkPostFormat('عندي سؤال سريع', body, [image]).error, 'format');
});
test('Title and body must agree on day', () => {
  assert.equal(checkPostFormat('يوم 2، تصميم واجهة متجر', body, [image]).ok, false);
});

test('retains six-character task and 35-character body minimums excluding URLs',()=>{
  const prefix='اليوم 1: ';
  const body35=prefix+'س'.repeat(35-prefix.length);
  assert.equal(checkPostFormat('يوم 1،دراسة',body35,[image]).error,'format');
  assert.equal(checkPostFormat('يوم 1،دراسةة',body35,[image]).ok,true);
  assert.equal(checkPostFormat('يوم 1،دراسةة',body35.slice(0,-1),[image]).error,'format');
  assert.equal(checkPostFormat('يوم 1،دراسةة',body35.slice(0,-2)+' '+image,[image]).error,'format');
});

test('format feedback identifies each failed requirement without changing acceptance rules',()=>{
  const good='اليوم 1: سأصمم واجهة المتجر وأراجع الألوان وأختبر الصفحات على الهاتف.';
  assert.match(checkPostFormat('عنوان بلا قالب',good,[image]).detail,/صيغة العنوان/);
  assert.match(checkPostFormat('يوم 1،عمل',good,[image]).detail,/6 أحرف/);
  assert.match(checkPostFormat('يوم 1، تصميم واجهة متجر',good.replace('اليوم 1','اليوم 2'),[image]).detail,/لا يطابق/);
  assert.match(checkPostFormat('يوم 1، تصميم واجهة متجر','اليوم 1: عمل',[image]).detail,/35 حرفًا/);
});
test('Image remains mandatory even when spot check is disabled', () => {
  assert.equal(checkPostFormat('يوم 1، تصميم واجهة متجر', body, []).error, 'missing_image');
});
test('Image URLs limited to Reddit hosted HTTPS', () => {
  assert.equal(redditImageUrl(image), image);
  assert.equal(redditImageUrl('http://i.redd.it/abc123.png'), undefined);
  assert.equal(redditImageUrl('https://evil.example/abc123.png'), undefined);
});
test('Extract image URLs without duplicates', () => {
  assert.deepEqual(extractImageUrls({ gallery: [{ url: image, status: 'VALID' }], body: `![](${image})` }), [image]);
});
test('Correctly distinguishes top comments vs threaded replies', () => {
  assert.equal(commentTarget('t3_post'), 'top_comment');
  assert.equal(commentTarget('t1_comment'), 'reply');
});
test('Sampling is deterministic and can be switched off/on', () => {
  assert.equal(shouldSampleImage('t3_abc', 0), false);
  assert.equal(shouldSampleImage('t3_abc', 100), true);
  assert.equal(shouldSampleImage('t3_abc', 5), shouldSampleImage('t3_abc', 5));
  assert.equal(shouldSampleImage('t3_abc', -3), false);
  assert.equal(shouldSampleImage('t3_abc', NaN), false);
});
test('Audit mode logs but does not touch content', () => {
  assert.equal(enforcement({ decision: 'remove', confidence: 1, reason_code: 'hostility', reason_ar: 'X' }, 'audit'), 'none');
});
test('Live mode automatically removes obvious trolls and structure failures', () => {
  assert.equal(enforcement({ decision: 'remove', confidence: 0.92, reason_code: 'trolling', reason_ar: 'X' }, 'live'), 'remove');
  assert.equal(enforcement({ decision: 'remove', confidence: 1, reason_code: 'format', reason_ar: 'X' }, 'live'), 'remove');
});
test('Review only for genuinely uncertain removals and explicit AI review', () => {
  assert.equal(enforcement({ decision: 'remove', confidence: 0.79, reason_code: 'other', reason_ar: 'X' }, 'live'), 'filter');
  assert.equal(enforcement({ decision: 'review', confidence: 0.2, reason_code: 'other', reason_ar: 'X' }, 'live'), 'filter');
});
test('Allows normal discussions, even when AI confidence is moderate', () => {
  assert.equal(enforcement({ decision: 'allow', confidence: 0.61, reason_code: 'ok', reason_ar: 'X' }, 'live'), 'none');
});
test('Review-only mode never removes content outright', () => {
  assert.equal(enforcement({ decision: 'remove', confidence: 1, reason_code: 'spam', reason_ar: 'X' }, 'review'), 'filter');
});
