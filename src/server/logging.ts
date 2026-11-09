/** Never copy provider errors (which may contain credentials or input) into logs. */
export function safeError(error: unknown): string {
  const message = error instanceof Error ? error.message : '';
  const http = /^OpenAI HTTP ([1-5][0-9]{2})$/.exec(message);
  if (http) return `OpenAI HTTP ${http[1]}`;
  const known = [
    'OpenAI API key missing', 'Invalid OpenAI response', 'OpenAI response not completed',
    'AI declined classification', 'No model verdict', 'Invalid verdict values',
    'Missing comment parent/post ID', 'Missing snapshot input', 'Lease expired before moderation action',
    'Decision history busy', 'App identity unavailable', 'Reply reconciliation incomplete',
  ];
  if (known.includes(message)) return message;
  if (error instanceof Error && ['TimeoutError', 'AbortError', 'SyntaxError'].includes(error.name)) return error.name;
  return 'Processing failed';
}

/** Short explanations are useful, but must not echo full inputs or credentials. */
export function safeReason(reason: string, inputs: Array<string | undefined>): string {
  let result = reason;
  for (const input of inputs) {
    if (input?.trim()) result = result.split(input).join('[content omitted]');
  }
  return result.replace(/\bBearer\s+\S+/gi, '[credential omitted]')
    .replace(/\bsk-[a-zA-Z0-9_-]+/g, '[credential omitted]').slice(0, 130);
}
