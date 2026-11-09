import assert from 'node:assert/strict';
import test from 'node:test';
import { DEFAULT_MODEL, makeModerationRequest, parseModerationResponse } from '../src/server/ai.ts';

const base = { target: 'post', postTitle: 'يوم 1، دراسة البرمجة', postBody: 'اليوم 1: أراجع درسًا وأكتب برنامجًا بسيطًا.', content: 'يوم 1، دراسة البرمجة' };
const imageUrl = 'https://i.redd.it/1234.jpg';

test('GPT-6 Luna is the API model ID, Responses API request is text-only by default', () => {
  const request = makeModerationRequest(base);
  assert.equal(DEFAULT_MODEL, 'gpt-6-luna');
  assert.equal(request.model, 'gpt-6-luna');
  assert.equal(request.reasoning.effort, 'none');
  assert.equal(request.store, false);
  assert.equal(request.input[0].content.length, 1);
  assert.equal(request.input[0].content[0].type, 'input_text');
  assert.equal(request.text.format.type, 'json_schema');
});

test('Only explicitly sampled posts attach an image', () => {
  const request = makeModerationRequest({ ...base, imageUrl });
  assert.equal(request.input[0].content[1].type, 'input_image');
  assert.equal(request.input[0].content[1].image_url, imageUrl);
  const reply = makeModerationRequest({ ...base, target: 'reply', imageUrl });
  assert.equal(reply.input[0].content.length, 1);
});

test('Parses Responses API structured JSON, including output list with other items', () => {
  const v = { decision: 'remove', confidence: 0.91, reason_code: 'trolling', reason_ar: 'منشور ساخر غير متعلق بالهدف.' };
  assert.deepEqual(parseModerationResponse({ status: 'completed', output: [{ type: 'reasoning' }, { type: 'message', content: [{ type: 'output_text', text: JSON.stringify(v) }] }] }), v);
});

test('Rejects incomplete, refused or malformed AI verdicts', () => {
  assert.throws(() => parseModerationResponse({ status: 'incomplete', output: [] }));
  assert.throws(() => parseModerationResponse({ status: 'completed', output: [{ content: [{ type: 'refusal' }] }] }));
  assert.throws(() => parseModerationResponse({ status: 'completed', output: [{ content: [{ type: 'output_text', text: '{"decision":"remove","confidence":4}' }] }] }));
});
