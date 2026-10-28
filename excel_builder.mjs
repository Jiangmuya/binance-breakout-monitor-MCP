import fs from 'node:fs/promises';
import path from 'node:path';
import {performance} from 'node:perf_hooks';
import {FormatCache,writeWorkbook} from './streaming_xlsx.mjs';

const c=(key,label,type='number',width=17)=>({key,label,type,width});
const identity=[c('symbol','交易对','text',20),c('contract_type','合约类型','text',24)];
const emas=[c('EMA12','EMA12','price'),c('EMA50','EMA50','price'),c('EMA200','EMA200','price')];
const checks=Array.from({length:6},(_,i)=>c(`check${i+1}`,['阳线突破','EMA条件','放量','实体扩张','涨幅扩张','阳线形态'][i],'bool',14));
const prices=['o','h','l','c'].map((k,i)=>c(k,['开盘价','最高价','最低价','收盘价'][i],'price'));
const dates=[c('t','开盘时间 北京','date',29),c('close_boundary','收盘边界 北京','date',29),c('t','开盘UTC毫秒','integer',22),c('T','API收盘UTC毫秒','integer',22)];
const triggerDates=dates.map(x=>x.key==='close_boundary'?{...x,label:'触发K线收盘时间 北京',width:32}:x);
const comparisons=[
 c('V_mean','比较均量'),c('V_median','比较中位量'),c('V_max','比较最大量'),c('volume_ratio','量比'),
 c('body','有符号实体','price'),c('abs_body','绝对实体','price'),c('relative_body','相对实体','rate'),
 c('B_mean','比较相对实体均值','rate'),c('relative_body_ratio','相对实体倍数'),
 c('abs_body_mean','比较实体均值','price'),c('abs_body_median','比较实体中位','price'),c('abs_body_max','比较实体最大','price'),
 c('abs_body_mean_ratio','实体/均值'),c('abs_body_median_ratio','实体/中位'),c('abs_body_max_ratio','实体/最大'),
 c('abs_body_vs_mean','实体减均值','price'),c('abs_body_vs_median','实体减中位','price'),c('abs_body_vs_max','实体减最大','price'),
 c('positive_count','比较阳线数','integer'),c('positive_body_mean','阳线实体均值','price'),
 c('bar_return','开收盘涨幅','rate'),c('close_return','相对前收盘涨幅','rate'),c('G_mean','比较平均涨幅','rate'),
 c('G_median','比较中位涨幅','rate'),c('G_max','比较最大涨幅','rate'),c('return_vs_mean','涨幅减均值','rate'),
 c('return_vs_max','涨幅减最大','rate'),c('H','比较最高价','price'),c('breakout','突破幅度','rate'),
 c('body_share','实体占比','rate'),c('upper_wick_share','上影占比','rate'),
 c('volume_rank','成交量排名','integer'),c('body_rank','实体排名','integer'),c('return_rank','涨幅排名','integer')];
const schemas={
 overview:[...identity,c('close_boundary','收盘边界 北京','date',29),c('c','收盘价','price'),...emas,
  c('ema_bullish','EMA多头排列','bool'),c('volume_ratio','量比'),c('relative_body_ratio','相对实体倍数'),c('bar_return','开收盘涨幅','rate'),
  ...checks,c('trigger','整体触发','bool'),c('readiness','预热状态','text',24),c('warmup','已积累K线','integer'),
  c('health','数据状态','text',22),c('stale','快照存在缺口','bool',20),c('updated','最后更新 北京','date',29),
  c('seed','EMA种子UTC毫秒','integer',22),c('t','开盘UTC毫秒','integer',22),c('T','API收盘UTC毫秒','integer',22),
  c('comparison_window','比较窗口根数','integer',20),c('strategy_version','策略版本','text',27),c('detail','状态说明','text',55),c('trigger_basis','触发依据','text',28)],
 triggers:[...identity,c('interval','周期','text'),...triggerDates,
  ...prices,c('v','基础币成交量'),c('q','USDT成交额'),...emas,...comparisons,...checks,
  c('trigger_basis','触发依据','text',28),c('event_type','事件类型','text',20),c('origin','数据来源阶段','text',20),c('readiness','预热状态','text',24),
  c('seed','EMA种子UTC毫秒','integer',22),c('warmup','累计K线数','integer'),c('discovered','触发发现时间 北京','date',29),
  c('discovered','发现UTC毫秒','integer',22),c('received','接收UTC毫秒','integer',22),c('processed','处理UTC毫秒','integer',22),c('event_time','交易所事件UTC毫秒','integer',24),
  c('notification_status','通知状态','text',24),c('sent','发送时间 北京','date',29),c('sent','发送UTC毫秒','integer',22),
  c('latency_ms','发现延迟毫秒','integer',20),c('sending_latency_ms','发送延迟毫秒','integer',20),c('attempts','通知尝试次数','integer'),
  c('thresholds','阈值JSON','text',70),c('comparison_window','比较窗口根数','integer',20),c('signal_id','signal_id','text',68),c('strategy_version','策略版本','text',27)],
 previous:[c('signal_id','signal_id','text',68),c('symbol','交易对','text',20),c('interval','周期','text'),c('index','比较K线序号','integer'),
  ...dates,...prices,c('v','基础币成交量'),c('q','USDT成交额'),c('abs_body','绝对实体','price'),c('relative_body','相对实体','rate'),
  c('bar_return','开收盘涨幅','rate'),c('close_return','相对前收盘涨幅','rate'),c('comparison_window','比较窗口根数','integer',20)],
 status:[c('time','记录时间 北京','date',29),c('symbol','交易对','text',20),c('interval','周期','text'),c('code','状态或异常','text',30),c('detail','详细说明','text',90),c('reason','排除原因','text',75)],
 parameters:[c('key','参数或口径','text',30),c('value','说明或值','text',125)]
};
const names={overview:'监控概览',triggers:'触发记录',previous:'历史K线对照',status:'数据状态与异常',parameters:'参数与说明'};
const sheetOrder=['triggers','overview','previous','status','parameters'];
const clean=v=>typeof v==='string'&&/^[=+@]/.test(v)?"'"+v:v;
// A process already running at upgrade time may still supply the v1 snapshot.
// Keep its 20-bar values and labels explicit rather than relabeling them as 15.
const normalize=(source,book)=>{
 const row={...source};
 // A missing path belongs to the legacy single-path strategy, not the new special rule.
 const triggerPath=row.trigger_path??(row.trigger===true||row.signal_id?'STANDARD':'NONE');
 row.trigger_basis=({STANDARD:'普通条件',SPECIAL:'特别条件',BOTH:'普通条件＋特别条件',NONE:'未触发'})[triggerPath]??'未记录';
 row.count??=row.occurrence_count;
 const window=row.comparison_window??('V15_mean'in row?15:'V20_mean'in row?20:book.comparison_window??20);
 row.comparison_window=window;
 for(const [prefix,suffixes]of [['V',['mean','median','max']],['B',['mean']],['G',['mean','median','max']],['abs_body',['mean','median','max']]]){
  for(const suffix of suffixes)row[`${prefix}_${suffix}`]??=row[`${prefix}${window}_${suffix}`];
 }
 row.H??=row[`H${window}`];
 // Preserve the decision/threshold saved with each strategy version.
 // Legacy 15-bar records without these fields retain their original 5x rule.
 row.volume_highlight=window===15&&(typeof row.volume_highlight==='boolean'?row.volume_highlight:
  row.V_mean!=null&&row.V_max!=null&&row.v>=(row.volume_highlight_multiple??5)*row.V_mean&&row.v>row.V_max);
 return row;
};

// Retain the original five sheet positions; a separate recent-anomaly sheet is
// included only in v4 inputs. Older saved snapshots remain readable.
schemas.recent_anomalies=[c('first_time','首次时间 北京','date',29),c('last_time','最近时间 北京','date',29),c('count','发生次数','integer',16),c('symbol','交易对','text',20),c('interval','周期','text',17),c('code','异常类型','text',30),c('detail','异常说明','text',90),c('signal_id','关联信号编号','text',68)];
names.recent_anomalies='近期异常';
const [input,out]=process.argv.slice(2);
if(!input||!out)throw new Error('Usage: node excel_builder.mjs input.json output-directory');
await fs.mkdir(out,{recursive:true});
const started=performance.now();
const metrics={format_version:4,writer:'streaming-ooxml/archiver-7.0.1',status:'RUNNING',duration_ms:0,peak_rss_bytes:0,peak_heap_bytes:0,books:[],cache_hits:0,cache_misses:0};
let memoryBudget=512*1024*1024,cache,timer,sampleError;
const sample=()=>{const m=process.memoryUsage();metrics.peak_rss_bytes=Math.max(metrics.peak_rss_bytes,m.rss);metrics.peak_heap_bytes=Math.max(metrics.peak_heap_bytes,m.heapUsed);if(m.rss>memoryBudget)throw new Error(`MEMORY_BUDGET_EXCEEDED: RSS ${m.rss} > ${memoryBudget}`);};
const check=()=>{if(sampleError)throw sampleError;sample();};
const positiveLimit=(value,label)=>{const number=Number(value);if(!Number.isSafeInteger(number)||number<1)throw new Error(`INVALID_BUDGET: ${label}`);return number;};
const recordFailure=error=>{metrics.status='FAILED';metrics.error??={name:error.name,message:error.message};process.exitCode=1;};
try{
  if((await fs.stat(input)).size>32*1024*1024)throw new Error('INPUT_ENVELOPE_TOO_LARGE: use NDJSON row sources');
  const spec=JSON.parse(await fs.readFile(input,'utf8'));
  if(!spec||!Array.isArray(spec.books))throw new Error('INVALID_INPUT_ENVELOPE: books must be an array');
  memoryBudget=positiveLimit(spec.budget?.rss_bytes??spec.memory_budget_bytes??memoryBudget,'rss_bytes');
  const batchRows=Math.min(5000,positiveLimit(spec.budget?.block_rows??spec.batch_rows??5000,'block_rows'));
  const batchBytes=Math.min(50*1024*1024,positiveLimit(spec.budget?.block_bytes??spec.batch_bytes??50*1024*1024,'block_bytes'));
  check();
  const cachePath=path.resolve(spec.format_cache??path.join(out,'formatted-rows.sqlite3'));
  await fs.mkdir(path.dirname(cachePath),{recursive:true});
  cache=new FormatCache(cachePath,'xlsx-format-v4-special-blue-20261028a');
  timer=setInterval(()=>{try{sample();}catch(error){sampleError??=error;}},100);
  for(const book of spec.books){
    check();
    if(path.basename(book.filename)!==book.filename||!book.filename.endsWith('.xlsx'))throw new Error('INVALID_OUTPUT_FILENAME');
    const order=spec.format_version>=4||book.recent_anomalies!=null?[...sheetOrder,'recent_anomalies']:sheetOrder;
    const result=await writeWorkbook(book,path.join(out,book.filename),{schemas,names,sheetOrder:order,normalize,cache,sample:check,batchRows,batchBytes});
    metrics.books.push(result);check();
  }
  metrics.status='SUCCESS';
}catch(error){recordFailure(error);}
finally{
  clearInterval(timer);
  if(cache){try{cache.close();}catch(error){recordFailure(error);}metrics.cache_hits=cache.hits;metrics.cache_misses=cache.misses;}
  metrics.duration_ms=Math.round(performance.now()-started);const m=process.memoryUsage();metrics.peak_rss_bytes=Math.max(metrics.peak_rss_bytes,m.rss);metrics.peak_heap_bytes=Math.max(metrics.peak_heap_bytes,m.heapUsed);
  await fs.writeFile(path.join(out,'metrics.json'),JSON.stringify(metrics,null,2));
  await fs.writeFile(path.join(out,'artifact-verification.json'),JSON.stringify(metrics.books.map(b=>({file:b.filename,rows:b.counts,errors:'',verification:'stream-write counts; reopen OOXML acceptance required'})),null,2));
}
console.log(JSON.stringify(metrics));
