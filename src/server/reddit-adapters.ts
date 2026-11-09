import { reddit, context } from '@devvit/web/server';
import type { ModItem } from './engine.ts';
import type { ItemId } from './queue.ts';
import type { NotificationPort } from './notifications.ts';

const getItem=(id:ItemId)=>id.startsWith('t3_')?reddit.getPostById(id as `t3_${string}`):reddit.getCommentById(id as `t1_${string}`);

export async function readModerationItem(id:ItemId):Promise<ModItem> {
  const item=await getItem(id);
  let appAuthored=false;
  if(id.startsWith('t1_')) {
    const appUser=await reddit.getAppUser();
    if(!appUser) throw new Error('App identity unavailable');
    appAuthored=item.authorName.toLowerCase()===appUser.username.toLowerCase();
  }
  // Preserve model getters/method receivers; spreading SDK models drops their getters.
  return new Proxy(item,{get(target,key){if(key==='appAuthored') return appAuthored;const value=Reflect.get(target,key,target) as unknown;return typeof value==='function'?value.bind(target):value;}}) as unknown as ModItem;
}

export const notificationPort:NotificationPort={
  async state(id) {
    const item=await getItem(id);
    return {approved:item.approved,deleted:item.authorName==='[deleted]'||item.body==='[deleted]',subreddit:item.subredditName};
  },
  async findReply(id,marker) {
    const appUser=await reddit.getAppUser();
    if(!appUser) throw new Error('App identity unavailable');
    const listing=id.startsWith('t3_')?(await reddit.getPostById(id as `t3_${string}`)).comments:(await reddit.getCommentById(id as `t1_${string}`)).replies;
    const replies=await listing.get(1000);
    const match=replies.find(comment=>comment.parentId===id&&comment.authorName.toLowerCase()===appUser.username.toLowerCase()&&comment.body.includes(marker));
    return {replyId:match?.id??null,complete:replies.length<1000};
  },
  async reply(id,text) {return (await reddit.submitComment({id,text,runAs:'APP'})).id;},
  async distinguish(id,sticky) {await (await reddit.getCommentById(id as `t1_${string}`)).distinguish(sticky);},
  async removalNote(id,text) {await reddit.addRemovalNote({itemIds:[id],reasonId:'',modNote:text});},
};

export async function authorizeModerator():Promise<boolean> {
  if(!context.userId||!context.subredditName) return false;
  const user=await reddit.getCurrentUser();
  if(!user||user.id!==context.userId) return false;
  const permissions=await user.getModPermissionsForSubreddit(context.subredditName);
  return permissions.length>0;
}

