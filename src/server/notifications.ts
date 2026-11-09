import { randomUUID } from 'node:crypto';
import { H, deletedKey, loadRecord, recordKey, saveRecord, type DecisionRecord } from './history.ts';
import { claim, releaseIfOwned, type ItemId, type RedisPort } from './queue.ts';
import { safeError } from './logging.ts';

export type NotificationPort = {
  state(id: ItemId): Promise<{ approved: boolean; deleted: boolean; subreddit: string }>;
  findReply(id: ItemId, marker: string): Promise<{ replyId: string | null; complete: boolean }>;
  reply(id: ItemId, text: string): Promise<string>;
  distinguish(replyId: string, sticky: boolean): Promise<void>;
  removalNote(id: ItemId, text: string): Promise<void>;
};

const RULES: Record<string,string> = {
  format:'صيغة التحديث اليومي',missing_image:'متطلبات المنشور',crosspost:'منع إعادة النشر',
  not_real_goal:'هدف يومي حقيقي',off_topic:'ارتباط المحتوى بهدف المجتمع',trolling:'منع الاستفزاز',
  spam:'منع الرسائل المزعجة والدعاية',hostility:'احترام الآخرين',not_encouraging:'التعليقات المباشرة الداعمة',
  image_mismatch:'متطلبات المنشور',other:'قواعد المجتمع',ok:'متوافق مع القواعد',
};
export const ruleLabel = (record: DecisionRecord) => RULES[record.verdict?.reason_code??'other']??'قواعد المجتمع';
export const noticeMarker = (record: DecisionRecord) => `DG-${recordKey(record.itemId,`${record.itemId}:${record.fingerprint}:${record.intendedAction}:${record.mode}`).split(':')[1]}`;
const escapeMarkdown = (text: string) => text.replace(/[\\`*_{}[\]()<>#!|]/g,'\\$&').replace(/[\r\n]+/g,' ');

export function explanation(record: DecisionRecord, subreddit: string): string {
  const pending=record.outcome==='reviewed';
  const status=pending?'تم إخفاء هذا المحتوى مؤقتًا وإحالته إلى المشرفين للمراجعة، ولم يصدر قرار نهائي بمخالفته.':'أزال دائبون هذا المحتوى من المجتمع.';
  const reason=record.source==='system_fallback'
    ?'تعذر إكمال الفحص الآلي بعد محاولات متكررة. هذا ليس حكمًا بمخالفة المحتوى.'
    :`السبب: ${escapeMarkdown(record.reason)}\n\nالقاعدة: ${ruleLabel(record)}.`;
  const fix=record.verdict?.reason_code==='format'
    ?'للتصحيح: اذكر رقم اليوم نفسه في العنوان والمتن، مع وصف للعمل لا يقل عن 6 أحرف بعد فاصل العنوان، ومتن لا يقل عن 35 حرفًا دون احتساب الروابط.\n\n**مثال كامل للمنشور:**\n\n**العنوان:** يوم 40، العمل على مشروع المتجر\n\n**المتن:**\n\nاليوم 40:\n\nهدفي اليوم إنهاء تصميم الصفحة الأولى وتجهيزها للعميل. إذا بقي وقت، سأبدأ العمل على الصفحات الداخلية.\n\nاستبدل رقم 40 برقم يومك الفعلي. هذا مثال إرشادي؛ لا يلزم استخدام كلمات أو عناوين فرعية محددة.'
    :record.verdict?.reason_code==='missing_image'?'للتصحيح: أرفق صورة للعمل أو لقطة شاشة لخطة اليوم مع النسخة الجديدة من منشورك، مع إبقاء العنوان والمتن المطلوبين.'
    :record.verdict?.reason_code==='crosspost'?'للتصحيح: انشر تحديثك الشخصي الأصلي بدل إعادة نشر محتوى مجتمع آخر.'
    :record.verdict?.reason_code==='not_encouraging'?'للتصحيح: اجعل تعليقك المباشر داعمًا أو قدم نصيحة عملية باحترام ولطف.'
    :'للتصحيح: راجع القاعدة المذكورة وصحّح المحتوى بما يتوافق معها.';
  const correctablePost=record.itemId.startsWith('t3_')&&['format','missing_image'].includes(record.verdict?.reason_code??'');
  const restoration=correctablePost
    ?'يمكنك نشر نسخة جديدة بعد التصحيح. تعديل المنشور المُزال وحده لن يعيده تلقائيًا. لمراجعة المنشور نفسه، تواصل مع المشرفين عبر Modmail.'
    :'تعديل المحتوى المُزال وحده لن يعيده تلقائيًا. لطلب مراجعة القرار أو استعادة المحتوى بعد التصحيح، تواصل مع المشرفين عبر Modmail.';
  return `${status}\n\n${reason}\n\n${pending?'يراجع المشرفون المحتوى؛ انتظر قرارهم ولا تعِد نشره أثناء المراجعة.':`${fix}\n\n${restoration}`}\n\nإذا اعتقدت أن القرار خاطئ، [تواصل مع المشرفين عبر Modmail](https://www.reddit.com/message/compose?to=${encodeURIComponent(`/r/${subreddit}`)}) وأرفق رابط المحتوى.\n\nمرجع القرار: ${noticeMarker(record)}`;
}

export async function deliverNotice(db: RedisPort, port: NotificationPort, key: string, now=Date.now()): Promise<void> {
  const owner=randomUUID();
  const lock=`dg3:notice-lock:${key.split(':')[0]}`;
  if(!await claim(db,lock,owner)) return;
  try {
    let record=await loadRecord(db,key,now);
    if(!record) {await db.zRem(H.notices,[key]);await db.zRem(H.failed,[key]);return;}
    if(!['removed','reviewed'].includes(record.outcome)) {await db.zRem(H.notices,[key]);return;}
    if(record.notification!=='pending'&&record.note!=='pending') {await db.zRem(H.notices,[key]);return;}
    if(record.deliveryAttempts>=4) {
      if(record.notification==='pending') record.notification='failed';
      if(record.note==='pending') record.note='failed';
      record.updatedAt=now;record.error='Delivery attempts exhausted; last result may be unresolved';
      await saveRecord(db,record);return;
    }
    record.updatedAt=now;
    record.deliveryAttempts++;
    if(!await saveRecord(db,record)) return;
    try {
      const state=await port.state(record.itemId);
      if(state.approved||state.deleted||await db.get(deletedKey(record.itemId))) {
        if(record.notification==='pending') record.notification='cancelled';
        if(record.note==='pending') record.note='cancelled';
        record.error=state.approved?'Moderator approved item':'Item deleted';
        await saveRecord(db,record);return;
      }
      if(record.notification==='pending') {
        if(!record.replyId) {
          const found=await port.findReply(record.itemId,noticeMarker(record));
          if(found.replyId) record.replyId=found.replyId;
          else {
            // Do not resend when a bounded reconciliation cannot establish absence.
            if(!found.complete) throw new Error('Reply reconciliation incomplete');
            record.sendStarted=true;
            if(!await saveRecord(db,record)) return;
            // Recheck cancellation immediately before the external write.
            const latest=await port.state(record.itemId);
            if(latest.approved||latest.deleted||await db.get(deletedKey(record.itemId))) {
              record.notification='cancelled';record.note='cancelled';await saveRecord(db,record);return;
            }
            record.replyId=await port.reply(record.itemId,explanation(record,state.subreddit));
          }
          if(!await saveRecord(db,record)) return;
        }
        await port.distinguish(record.replyId,record.itemId.startsWith('t3_'));
        record.notification='sent';record.error=null;
        if(!await saveRecord(db,record)) return;
      }
      if(record.note==='pending') {
        await port.removalNote(record.itemId,`${noticeMarker(record)} ${record.reason}`.slice(0,100));
        record.note='sent';record.error=null;
      }
      await saveRecord(db,record);
      console.log(JSON.stringify({event:'dg3_notice',id:record.itemId,decision:key,notification:record.notification,note:record.note,attempts:record.deliveryAttempts}));
    } catch(error) {
      // Reload to retain a reply ID persisted before a later failure.
      record=await loadRecord(db,key,now)??record;
      record.updatedAt=now;record.error=safeError(error);
      if(record.deliveryAttempts>=4) {
        if(record.notification==='pending') record.notification='failed';
        if(record.note==='pending') record.note='failed';
      }
      await saveRecord(db,record);
      if(record.notification==='pending'||record.note==='pending') await db.zAdd(H.notices,{member:key,score:now+60000});
      console.error(JSON.stringify({event:'dg3_notice_error',id:record.itemId,decision:key,attempts:record.deliveryAttempts,error:record.error}));
    }
  } finally {await releaseIfOwned(db,lock,owner);}
}

export async function drainNotices(db: RedisPort, port: NotificationPort, now=Date.now()): Promise<number> {
  const rows=await db.zRange(H.notices,0,now,{by:'score',limit:{offset:0,count:5}});
  await Promise.all(rows.map(({member})=>deliverNotice(db,port,member,now)));
  return rows.length;
}
