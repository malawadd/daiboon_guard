import { Hono } from 'hono';
import type { UiResponse } from '@devvit/web/shared';
import type { FormField } from '@devvit/shared-types/shared/form.js';
import { H, loadRecord, recentRecords, type DecisionRecord } from './history.ts';
import { metrics, type ItemId, type RedisPort } from './queue.ts';
import { ruleLabel } from './notifications.ts';
import { safeError } from './logging.ts';

const outcomes: Record<DecisionRecord['outcome'],string> = {
  unresolved:'نتيجة الإجراء غير مؤكدة',removed:'تمت الإزالة',reviewed:'بانتظار مراجعة المشرفين',
  allowed:'مسموح',audit:'تدقيق فقط',skipped:'تم التجاوز',superseded:'قرار ملغى بسبب تعديل المحتوى',failed:'فشل الفحص',
};
const sources: Record<DecisionRecord['source'],string> = {format:'قواعد برمجية',ai:'ذكاء اصطناعي',cache:'قرار مخزن',system_fallback:'إجراء أمان',system:'النظام'};
const delivery: Record<DecisionRecord['notification'],string> = {not_needed:'غير مطلوب',pending:'قيد الانتظار',sent:'تم',failed:'فشل',cancelled:'ملغى'};
const modeLabels={live:'تلقائي',review:'مراجعة',audit:'تدقيق'};
const actionLabels={none:'لا إجراء',remove:'إزالة',filter:'إحالة للمراجعة'};
const reasonLabels:Record<string,string>={cached_completion:'تم فحص هذا المحتوى سابقًا',already_moderated:'سبق اتخاذ إجراء إشرافي على المحتوى',moderated_before_action:'تدخل المشرف قبل تنفيذ القرار',content_changed:'تغيّر المحتوى قبل تنفيذ القرار'};
const displayReason=(record:DecisionRecord)=>reasonLabels[record.reason]??record.reason;
export function decisionText(record: DecisionRecord): string {
  return [
    `الحالة: ${outcomes[record.outcome]}`,
    `السبب: ${displayReason(record)}`,`القاعدة: ${ruleLabel(record)}`,
    `المصدر: ${sources[record.source]}`,`النمط: ${record.mode?modeLabels[record.mode]:'غير متاح'}`,
    `القرار: ${record.verdict?.decision??'غير متاح'} | الثقة: ${record.verdict?.confidence??'غير متاحة'}`,
    `النموذج: ${record.model??'غير مستخدم'}`,`الإجراء المقصود: ${actionLabels[record.intendedAction]}`,
    `الوقت: ${new Date(record.createdAt).toISOString()}`,
    `إشعار الكاتب: ${delivery[record.notification]} | ملاحظة المشرفين: ${delivery[record.note]}`,
    `محاولات الإرسال: ${record.deliveryAttempts} | الرد: ${record.replyId??'لا يوجد'}`,
    `الخطأ: ${record.error??'لا يوجد'}`,`المحتوى: ${record.itemId}`,`مرجع: ${record.key}`,
  ].join('\n\n');
}
const validId=(value:unknown):value is ItemId=>typeof value==='string'&&/^t[13]_[a-zA-Z0-9]+$/.test(value);
const validKey=(value:unknown):value is string=>typeof value==='string'&&/^t[13]_[a-zA-Z0-9]+:[a-f0-9]{24}$/.test(value);
const timeLabel=(time:number)=>new Intl.DateTimeFormat('ar-SA',{timeZone:'Asia/Riyadh',month:'short',day:'numeric',hour:'2-digit',minute:'2-digit'}).format(time);

export function moderatorRoutes(deps:{db:RedisPort;authorize:()=>Promise<boolean>;now?:()=>number}) {
  const routes=new Hono();
  routes.onError((error,c)=>{
    console.error(JSON.stringify({event:'dg3_mod_view_error',error:safeError(error)}));
    return c.json<UiResponse>({showToast:'تعذر عرض البيانات. حاول لاحقًا.'},503);
  });
  routes.use('*',async(c,next)=>{
    try {
      if(!await deps.authorize()) return c.json<UiResponse>({showToast:'هذه المعلومات متاحة لمشرفي المجتمع فقط.'},403);
      await next();
    } catch(error) {
      console.error(JSON.stringify({event:'dg3_mod_view_error',error:safeError(error)}));
      return c.json<UiResponse>({showToast:'تعذر عرض البيانات. حاول لاحقًا.'},503);
    }
  });
  const details=(record:DecisionRecord|null,page=0):UiResponse=>({showForm:{name:'dgDecision',form:{
    title:'قرار دائبون',
    description:record?decisionText(record):'لا يوجد قرار محفوظ لهذا المحتوى (No stored decision). يبدأ السجل مع هذا الإصدار، ويحتفظ بالقرارات لمدة تصل إلى 14 يومًا.',
    fields:[{name:'navigation',label:'بعد قراءة القرار',type:'select',options:[{label:'إغلاق',value:'close'},{label:'العودة إلى النشاط',value:`back:${page}`}],defaultValue:['close']}],
    acceptLabel:'تنفيذ',cancelLabel:'إغلاق',
  }}});
  routes.post('/decision',async c=>{
    const body=await c.req.json<{targetId?:unknown}>();
    if(!validId(body.targetId)) return c.json<UiResponse>({showToast:'معرّف المحتوى غير صحيح.'},400);
    const {records}=await recentRecords(deps.db,0,body.targetId,deps.now?.()??Date.now());
    // A duplicate trigger skip must not hide the preceding substantive decision.
    const substantive=records.find(r=>r.outcome!=='skipped')??records[0];
    return c.json<UiResponse>(details(substantive??null));
  });

  const activity=async(page:number,id?:ItemId):Promise<UiResponse>=>{
    const now=deps.now?.()??Date.now();
    const {records,total}=await recentRecords(deps.db,page,id,now);
    const health=await metrics(deps.db);
    const [failed,pending]=await Promise.all([deps.db.zCard(H.failed),deps.db.zCard(H.notices)]);
    const description=`أعمال الفحص المنتظرة: ${health.pending} | متعثرة: ${health.dead}\n\nإشعارات منتظرة: ${pending} | فاشلة: ${failed}\n\nالصفحة ${page+1} من ${Math.max(1,Math.ceil(total/20))}\n\n${records.length?'اختر قرارًا من القائمة ثم اضغط «فتح / تنفيذ» لعرض التفاصيل كاملة.':'لا توجد قرارات محفوظة في هذه الصفحة.'}`;
    const fields:FormField[]=[];
    if(records.length) fields.push({name:'decisionKey',label:'القرارات — الحالة، السبب المختصر، الوقت',type:'select',
      options:records.map(record=>({value:record.key,label:`${outcomes[record.outcome]} | ${displayReason(record).replace(/[\r\n]+/g,' ').slice(0,45)} | ${timeLabel(record.createdAt)} | ${record.itemId}`})),defaultValue:[records[0]!.key]});
    fields.push({name:'itemId',label:'بحث بمعرّف منشور أو تعليق — اختر «تحديث / بحث» بعد إدخاله',type:'string',defaultValue:id??''});
    fields.push({name:'navigation',label:'الإجراء',type:'select',options:[{label:'فتح القرار المحدد',value:`open:${page}`},{label:'تحديث / بحث',value:`refresh:${page}`},{label:'التالي',value:`next:${page}`},{label:'السابق',value:`previous:${page}`}],defaultValue:[`${records.length?'open':'refresh'}:${page}`]});
    return {showForm:{name:'dgActivity',form:{title:'نشاط دائبون',description,fields,acceptLabel:'فتح / تنفيذ',cancelLabel:'إغلاق'}}};
  };
  routes.post('/close',async c=>{
    const body=await c.req.json<{navigation?:unknown}>();
    const selection=Array.isArray(body.navigation)?body.navigation[0]:body.navigation;
    const back=typeof selection==='string'?/^back:([0-9]{1,2})$/.exec(selection):null;
    if(back&&Number(back[1])<=49) return c.json<UiResponse>(await activity(Number(back[1])));
    return c.json<UiResponse>({showToast:'تم إغلاق تفاصيل القرار.'});
  });
  routes.post('/activity',async c=>c.json<UiResponse>(await activity(0)));
  routes.post('/activity-submit',async c=>{
    const body=await c.req.json<{page?:unknown;itemId?:unknown;navigation?:unknown;decisionKey?:unknown}>();
    const raw=typeof body.itemId==='string'?body.itemId.trim():'';
    if(raw&&!validId(raw)) return c.json<UiResponse>({showToast:'استخدم معرّفًا مثل t3_abc أو t1_abc.'},400);
    const selection=Array.isArray(body.navigation)?body.navigation[0]:body.navigation;
    const navigation=typeof selection==='string'?/^(open|next|previous|refresh):([0-9]{1,2})$/.exec(selection):null;
    // Accept forms opened before this hotfix as well as the new encoded navigation.
    const action=navigation?.[1]??selection;
    const page=Number(navigation?.[2]??body.page??0);
    if(!Number.isInteger(page)||page<0||page>49) return c.json<UiResponse>({showToast:'رقم الصفحة غير صحيح.'},400);
    if(action==='open') {
      const key=Array.isArray(body.decisionKey)?body.decisionKey[0]:body.decisionKey;
      if(!validKey(key)) return c.json<UiResponse>({showToast:'اختر قرارًا من القائمة.'},400);
      return c.json<UiResponse>(details(await loadRecord(deps.db,key,deps.now?.()??Date.now()),page));
    }
    const next=action==='refresh'?0:Math.max(0,Math.min(49,page+(action==='next'?1:action==='previous'?-1:0)));
    return c.json<UiResponse>(await activity(next,validId(raw)?raw:undefined));
  });
  return routes;
}
