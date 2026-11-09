/** Pure helpers: safe to unit-test without Reddit or an AI key. */
export type Decision = 'allow' | 'review' | 'remove';
export type Target = 'post' | 'top_comment' | 'reply';
export type ReasonCode =
  | 'ok' | 'format' | 'missing_image' | 'crosspost' | 'not_real_goal' | 'off_topic'
  | 'trolling' | 'spam' | 'hostility' | 'not_encouraging' | 'image_mismatch' | 'other';

export type Verdict = {
  decision: Decision;
  confidence: number;
  reason_code: ReasonCode;
  reason_ar: string;
};

export type ImageInput = {
  url?: string;
  gallery?: Array<{ url: string; status?: string | number }>;
  body?: string;
  bodyHtml?: string;
};

/** Translate Arabic-Indic and Eastern Arabic digits to ASCII digits. */
export function normalizeDigits(value: string): string {
  return value.replace(/[\u0660-\u0669\u06f0-\u06f9]/g, (digit) => {
    const code = digit.charCodeAt(0);
    return String(code >= 0x06f0 ? code - 0x06f0 : code - 0x0660);
  });
}

/** Accept Reddit-hosted image URLs for required attachment detection and optional spot checks. */
export function redditImageUrl(raw: string): string | undefined {
  try {
    const url = new URL(raw.replace(/&amp;/g, '&'));
    if (url.protocol !== 'https:') return undefined;
    if (!['i.redd.it', 'preview.redd.it', 'external-preview.redd.it',
      'external-i.redd.it', 'reddit.com', 'www.reddit.com'].includes(url.hostname)) return undefined;
    if (url.hostname.endsWith('reddit.com') && !url.pathname.startsWith('/media')) return undefined;
    return url.toString();
  } catch {
    return undefined;
  }
}

export function extractImageUrls(input: ImageInput): string[] {
  const candidates: string[] = [];
  for (const media of input.gallery ?? []) {
    // Ignore failed processing; treat unreported statuses as potentially valid.
    if (!media.status || media.status === 'VALID' || media.status === 1) candidates.push(media.url);
  }
  if (input.url) candidates.push(input.url);
  const source = `${input.body ?? ''}\n${input.bodyHtml ?? ''}`;
  // Reddit rich text may surface media as URLs, markdown, or HTML tags.
  for (const match of source.matchAll(/https:\/\/(?:i\.redd\.it|preview\.redd\.it|external-preview\.redd\.it|external-i\.redd\.it)\/[\w%./?&=+~-]+/g)) {
    candidates.push(match[0]);
  }
  return [...new Set(candidates.map(redditImageUrl).filter((v): v is string => !!v))].slice(0, 4);
}

export type PostCheck = {
  ok: boolean;
  day?: number;
  error?: ReasonCode;
  detail?: string;
};

export function checkPostFormat(title: string, body: string | undefined, imageUrls: string[]): PostCheck {
  const normalizedTitle = normalizeDigits(title).trim();
  // يوم 1، العمل على مشروع فلان | يوم ١: تصميم واجهة متجر
  const match = /^يوم\s+([0-9]{1,5})\s*[,،:\-–—]\s*(.{6,})$/u.exec(normalizedTitle);
  if (!match || Number(match[1]) < 1) {
    const prefix=/^يوم\s+([0-9]{1,5})\s*[,،:\-–—]/u.exec(normalizedTitle);
    return { ok: false, error: 'format', detail: prefix&&Number(prefix[1])>=1
      ?'وصف العمل بعد فاصل العنوان أقصر من الحد المطلوب: 6 أحرف.'
      :'صيغة العنوان غير صحيحة: ابدأ بـ «يوم 1، العمل على ...» برقم يوم موجب.' };
  }
  const day = Number(match[1]);
  const text = normalizeDigits(body ?? '').trim();
  const dayInBody = new RegExp(`(?:اليوم|يوم)\\s*[:：-]?\\s*${day}(?!\\d)`, 'u');
  if (!dayInBody.test(text)) return {ok:false,day,error:'format',detail:'رقم اليوم في المتن مفقود أو لا يطابق رقم اليوم في العنوان.'};
  if (text.replace(/https?:\/\/\S+/g, '').length < 35) return {ok:false,day,error:'format',detail:'متن المنشور أقصر من 35 حرفًا بعد استبعاد الروابط؛ اذكر هدفك اليومي بوضوح.'};
  if (imageUrls.length === 0) {
    return { ok: false, day, error: 'missing_image', detail: 'أرفق صورة للعمل أو لقطة شاشة لخطة اليوم.' };
  }
  return { ok: true, day };
}

/** Reddit parent IDs start t3_ for top-level comments, t1_ for replies. */
export function commentTarget(parentId: string): 'top_comment' | 'reply' {
  if (parentId.startsWith('t3_')) return 'top_comment';
  if (parentId.startsWith('t1_')) return 'reply';
  throw new Error('Unknown Reddit parent ID');
}

/** Stable sample decision by post ID; edits keep the same sampling decision. */
export function shouldSampleImage(postId: string, percentage: number): boolean {
  const rate = Number.isFinite(percentage) ? Math.min(100, Math.max(0, percentage)) : 0;
  if (rate <= 0) return false;
  if (rate >= 100) return true;
  // FNV-1a (32-bit), deterministic over the complete Reddit post ID.
  let hash = 2166136261;
  for (let i = 0; i < postId.length; i++) {
    hash = Math.imul(hash ^ postId.charCodeAt(i), 16777619) >>> 0;
  }
  return (hash / 0x100000000) * 100 < rate;
}

export type Mode = 'audit' | 'review' | 'live';
export function enforcement(verdict: Verdict, mode: Mode): 'none' | 'filter' | 'remove' {
  if (mode === 'audit') return 'none';
  if (verdict.decision === 'allow') return 'none';
  if (mode === 'review') return 'filter';
  // Live: auto-remove likely/clear violations. Reserve review for exceptional ambiguity.
  // AI self-reported confidence is NOT calibrated: audit appeals and revise based on real data.
  if (verdict.decision === 'remove' && verdict.confidence >= 0.80) return 'remove';
  return 'filter';
}
