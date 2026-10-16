'use strict';
// Display conversion only; routing and upstream billing retain the original amounts.
function balanceCny(quota){
  if(quota.remaining==null||!Number.isFinite(Number(quota.remaining)))return '—';
  const currency=String(quota.currency||'').toUpperCase();
  const rate=currency==='USD'?7:['CNY','RMB'].includes(currency)?1:null;
  return rate==null?'CNY 换算不可用':`${(Number(quota.remaining)*rate).toLocaleString('zh-CN',{maximumFractionDigits:4})} CNY`;
}
function testStillFailed(source){
  const test=source.last_test;
  return !!test&&!test.passed&&!(Date.parse(source.routing?.last_success_at)>Date.parse(test.checked_at));
}
function probeSchedule(item,now=Date.now()/1000){
  if(item.probe_in_progress)return '正在探测恢复';
  if(['open','half_open'].includes(item.state)){
    return item.until>now?'下次探测 '+routingTime(item.until):'冷却已结束，等待恢复探测';
  }
  if((item.state||'closed')==='closed'&&item.consecutive>0)return '未进入熔断，下次请求可重试';
  return '';
}
function routingClockPhase(sources){
  const now=Date.now()/1000;
  return sources.flatMap(s=>[s.routing||{},...Object.values(s.model_routing||{})]).map(r=>`${r.until>now}:${r.next_check>now}`).join('|');
}
function sourceAppearance(source){
  const r=source.routing||{},q=source.quota||{},v=r.state||'closed';
  if(!source.enabled)return {tone:'neutral',label:'已暂停'};
  if(v==='auth_required'||source.kind==='oauth'&&!source.authorized)return {tone:'error',label:'需要授权'};
  if(['quota_wait','balance_wait'].includes(v)||q.excluded||q.status==='exhausted')return {tone:'waiting',label:'等待额度恢复'};
  if(v==='open'||r.consecutive>0)return {tone:'error',label:'请求异常'};
  if(v==='half_open')return {tone:'waiting',label:'恢复验证中'};
  if(Object.keys(source.model_routing||{}).length)return {tone:'error',label:'部分模型异常'};
  if(r.reserve_enabled&&(r.reserve_blocked||!r.reserve_known||r.query_error))return {tone:'waiting',label:'额度保护中'};
  if(testStillFailed(source))return {tone:'error',label:'最近测试失败'};
  if(source.kind==='api'&&q.status!=='available')return {tone:'waiting',label:'余额待确认'};
  if(r.query_error)return {tone:'waiting',label:'额度待确认'};
  if(r.last_success_at||source.last_test?.passed)return {tone:'healthy',label:'请求正常'};
  return {tone:'neutral',label:'等待请求验证'};
}
function statusIcon(name){
  const paths={
    healthy:'<circle cx="12" cy="12" r="9"/><path d="m8 12 2.5 2.5L16 9"/>',
    error:'<circle cx="12" cy="12" r="9"/><path d="m9 9 6 6m0-6-6 6"/>',
    partial:'<path d="M10.3 4.9 2.9 18a2 2 0 0 0 1.7 3h14.8a2 2 0 0 0 1.7-3L13.7 4.9a2 2 0 0 0-3.4 0Z"/><path d="M12 9v4m0 4h.01"/>',
    waiting:'<circle cx="12" cy="12" r="9"/><path d="M12 7v5l3 2"/>',
    reserve:'<path d="m12 3 8 3v6c0 5-8 9-8 9s-8-4-8-9V6l8-3Z"/><path d="M9 12h6"/>',
    neutral:'<circle cx="12" cy="12" r="9"/><path d="M12 8v4m0 4h.01"/>',
    paused:'<circle cx="12" cy="12" r="9"/><path d="M9 8v8m6-8v8"/>',
    up:'<path d="m6 15 6-6 6 6"/>',down:'<path d="m6 9 6 6 6-6"/>',
    overview:'<rect x="3" y="3" width="7" height="7" rx="1.5"/><rect x="14" y="3" width="7" height="7" rx="1.5"/><rect x="3" y="14" width="7" height="7" rx="1.5"/><rect x="14" y="14" width="7" height="7" rx="1.5"/>',
    models:'<path d="m12 3 9 5-9 5-9-5 9-5Zm-9 9 9 5 9-5M3 16l9 5 9-5"/>',
    access:'<path d="m8 8-4 4 4 4m8-8 4 4-4 4m-3-11-2 14"/>',
    logs:'<rect x="4" y="3" width="16" height="18" rx="2"/><path d="M8 8h8M8 12h8m-8 4h5"/>',
    activity:'<path d="M3 12h4l3-8 4 16 3-8h4"/>',
    refresh:'<path d="M20 7v5h-5M4 17v-5h5"/><path d="M6 7a7 7 0 0 1 11-1l3 3M4 15l3 3a7 7 0 0 0 11-1"/>',
    copy:'<rect x="8" y="8" width="12" height="13" rx="2"/><path d="M16 8V5a2 2 0 0 0-2-2H5a2 2 0 0 0-2 2v9a2 2 0 0 0 2 2h3"/>',
    key:'<circle cx="8" cy="8" r="5"/><path d="m12 12 9 9m-6-6 3-3m0 6 3-3"/>',
    close:'<path d="m6 6 12 12M6 18 18 6"/>',
    moon:'<path d="M20.5 13.5A9 9 0 0 1 10.5 3a9 9 0 1 0 10 10.5Z"/>',
    sun:'<circle cx="12" cy="12" r="4"/><path d="M12 2v2m0 16v2M2 12h2m16 0h2M5 5l1.5 1.5m11 11L19 19M5 19l1.5-1.5m11-11L19 5"/>'
  };
  return `<svg class="status-icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.7" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true" focusable="false">${paths[name]||paths.neutral}</svg>`;
}
function sourceStatus(source){
  const a=sourceAppearance(source),r=source.routing||{},q=source.quota||{};
  let icon=a.tone,reason='',label=a.label;
  if(!source.enabled){icon='paused';reason='已停止参与自动选路'}
  else if(label==='需要授权'){reason='登录已失效或尚未完成授权'}
  else if(label==='等待额度恢复'){
    reason=r.state==='quota_wait'?'官方额度已耗尽':'上游余额已耗尽';
    if(r.reset_at)reason+=' · 预计 '+routingTime(r.reset_at)+' 恢复';
  }else if(label==='请求异常'){
    reason=routingLabel(r.reason);
    const schedule=probeSchedule(r);if(schedule)reason+=' · '+schedule;
  }else if(label==='部分模型异常'){
    icon='partial';const entries=Object.values(source.model_routing||{});
    reason=`${entries.length} 个模型异常 · ${routingLabel(entries[0]?.reason)}`;
  }else if(label==='恢复验证中'){reason=probeSchedule(r)||'正在验证上游是否恢复'}
  else if(label==='额度保护中'){
    icon='reserve';reason=r.reserve_blocked?`已达 ${r.reserve_percent}% 预留线`:'剩余额度暂不能确认，保护性跳过';
    if(r.reserve_blocked&&r.reserve_reset_at)reason+=' · 预计 '+routingTime(r.reserve_reset_at)+' 恢复';
  }else if(label==='最近测试失败'){
    label='请求异常';reason=`HTTP ${source.last_test?.http||'—'} · ${source.last_test?.error||'最近测试未通过'}`;
  }else if(a.tone==='healthy'){label='正常';reason='最近请求验证通过'}
  else if(label==='余额待确认'){reason=q.error?'余额查询失败，等待重新查询':'尚未取得有效余额'}
  else if(label==='额度待确认'){reason='官方额度查询暂不可用'}
  else{label='待验证';reason='尚无有效请求验证结果'}
  return {...a,label,icon,reason};
}
