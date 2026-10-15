import fs from 'node:fs/promises';
import path from 'node:path';
import os from 'node:os';
import {createRequire} from 'node:module';
import {pathToFileURL} from 'node:url';

// Resolve the bundled authoring package without modifying its installation.
const root=process.env.ARTIFACT_TOOL_MODULE_ROOT || path.join(os.homedir(),'.cache/codex-runtimes/codex-primary-runtime/dependencies/node');
const requireBundle=createRequire(path.join(root,'package.json'));
const {Workbook,SpreadsheetFile}=await import(pathToFileURL(requireBundle.resolve('@oai/artifact-tool')).href);
const [input,out]=process.argv.slice(2);
const spec=JSON.parse(await fs.readFile(input,'utf8'));
await fs.mkdir(out,{recursive:true});
const serial=ms=>ms==null?null:(ms+28800000)/86400000+25569;
const col=n=>{let s='';for(n++;n;n=Math.floor((n-1)/26))s=String.fromCharCode(65+(n-1)%26)+s;return s;};
const c=(key,label,type='number',width=17)=>({key,label,type,width});
const identity=[c('symbol','交易对','text',20),c('contract_type','合约类型','text',24)];
const emas=[c('EMA12','EMA12','price'),c('EMA50','EMA50','price'),c('EMA200','EMA200','price')];
const checks=Array.from({length:6},(_,i)=>c(`check${i+1}`,['阳线突破','EMA条件','放量','实体扩张','涨幅扩张','阳线形态'][i],'bool',14));
const prices=['o','h','l','c'].map((k,i)=>c(k,['开盘价','最高价','最低价','收盘价'][i],'price'));
const dates=[c('t','开盘时间 北京','date',29),c('close_boundary','收盘边界 北京','date',29),c('t','开盘UTC毫秒','integer',22),c('T','API收盘UTC毫秒','integer',22)];
const triggerDates=dates.map(x=>x.key==='close_boundary'?{...x,label:'触发K线收盘时间 北京',width:32}:x);
const comparisons=[
 c('V20_mean','前20均量'),c('V20_median','前20中位量'),c('V20_max','前20最大量'),c('volume_ratio','量比'),
 c('body','有符号实体','price'),c('abs_body','绝对实体','price'),c('relative_body','相对实体','rate'),
 c('B20_mean','前20相对实体均值','rate'),c('relative_body_ratio','相对实体倍数'),
 c('abs_body20_mean','前20实体均值','price'),c('abs_body20_median','前20实体中位','price'),c('abs_body20_max','前20实体最大','price'),
 c('abs_body_mean_ratio','实体/均值'),c('abs_body_median_ratio','实体/中位'),c('abs_body_max_ratio','实体/最大'),
 c('abs_body_vs_mean','实体减均值','price'),c('abs_body_vs_median','实体减中位','price'),c('abs_body_vs_max','实体减最大','price'),
 c('positive_count','前20阳线数','integer'),c('positive_body_mean','阳线实体均值','price'),
 c('bar_return','开收盘涨幅','rate'),c('close_return','相对前收盘涨幅','rate'),c('G20_mean','前20平均涨幅','rate'),
 c('G20_median','前20中位涨幅','rate'),c('G20_max','前20最大涨幅','rate'),c('return_vs_mean','涨幅减均值','rate'),
 c('return_vs_max','涨幅减最大','rate'),c('H20','前20最高价','price'),c('breakout','突破幅度','rate'),
 c('body_share','实体占比','rate'),c('upper_wick_share','上影占比','rate'),
 c('volume_rank','成交量排名/21','integer'),c('body_rank','实体排名/21','integer'),c('return_rank','涨幅排名/21','integer')];
const schemas={
 overview:[...identity,c('close_boundary','收盘边界 北京','date',29),c('c','收盘价','price'),...emas,
  c('ema_bullish','EMA多头排列','bool'),c('volume_ratio','量比'),c('relative_body_ratio','相对实体倍数'),c('bar_return','开收盘涨幅','rate'),
  ...checks,c('trigger','整体触发','bool'),c('readiness','预热状态','text',24),c('warmup','已积累K线','integer'),
  c('health','数据状态','text',22),c('stale','快照存在缺口','bool',20),c('updated','最后更新 北京','date',29),
  c('seed','EMA种子UTC毫秒','integer',22),c('t','开盘UTC毫秒','integer',22),c('T','API收盘UTC毫秒','integer',22),
  c('strategy_version','策略版本','text',27),c('detail','状态说明','text',55)],
 triggers:[...identity,c('interval','周期','text'),...triggerDates,
  ...prices,c('v','基础币成交量'),c('q','USDT成交额'),...emas,...comparisons,...checks,
  c('event_type','事件类型','text',20),c('origin','数据来源阶段','text',20),c('readiness','预热状态','text',24),
  c('seed','EMA种子UTC毫秒','integer',22),c('warmup','累计K线数','integer'),c('discovered','触发发现时间 北京','date',29),
  c('discovered','发现UTC毫秒','integer',22),c('received','接收UTC毫秒','integer',22),c('processed','处理UTC毫秒','integer',22),c('event_time','交易所事件UTC毫秒','integer',24),
  c('notification_status','通知状态','text',24),c('sent','发送时间 北京','date',29),c('sent','发送UTC毫秒','integer',22),
  c('latency_ms','发现延迟毫秒','integer',20),c('sending_latency_ms','发送延迟毫秒','integer',20),c('attempts','通知尝试次数','integer'),
  c('thresholds','阈值JSON','text',70),c('signal_id','signal_id','text',68),c('strategy_version','策略版本','text',27)],
 previous:[c('signal_id','signal_id','text',68),c('symbol','交易对','text',20),c('interval','周期','text'),c('index','前20序号','integer'),
  ...dates,...prices,c('v','基础币成交量'),c('q','USDT成交额'),c('abs_body','绝对实体','price'),c('relative_body','相对实体','rate'),
  c('bar_return','开收盘涨幅','rate'),c('close_return','相对前收盘涨幅','rate')],
 status:[c('time','记录时间 北京','date',29),c('symbol','交易对','text',20),c('interval','周期','text'),c('code','状态或异常','text',30),c('detail','详细说明','text',90),c('reason','排除原因','text',75)],
 parameters:[c('key','参数或口径','text',30),c('value','说明或值','text',125)]
};
const names={overview:'监控概览',triggers:'触发记录',previous:'前20根对照',status:'数据状态与异常',parameters:'参数与说明'};
const sheetOrder=['triggers','overview','previous','status','parameters'];
const clean=v=>typeof v==='string'&&/^[=+@]/.test(v)?"'"+v:v;
const verification=[];
for(const book of spec.books){
 const wb=Workbook.create();
 // The first added worksheet is also the workbook's default opening sheet.
 for(const key of sheetOrder){
  const columns=schemas[key];
  const rows=book[key],s=wb.worksheets.add(names[key]);
  const n=Math.max(rows.length,1),last=6+n;
  s.showGridLines=false;
  const all=s.getRangeByIndexes(0,0,last,columns.length);
  all.format.font={name:'Microsoft YaHei',size:10,color:'#22334A'};
  all.format.rowHeight=23;all.format.verticalAlignment='center';
  s.getRange('A1').values=[[`${names[key]} · ${book.interval}`]];
  s.getRange('A1').format.font={name:'Microsoft YaHei',size:17,bold:true,color:'#15344F'};
  s.getRange('A1').format.rowHeight=34;
  s.getRange('A2').values=[['只读监控；真实行情快照。条件触发不代表实际交易。']];
  s.getRange('A3').values=[['快照时间 北京']];s.getRange('B3').values=[[serial(book.generated)]];
  s.getRange('B3').setNumberFormat('yyyy-mm-dd hh:mm:ss');
  const recordSummary=rows.length?`本表记录 ${rows.length} 条；数值来自持久化数据库。`:'本次无记录；未填入任何模拟信号。';
  const coverage=book.coverage;
  const coverageSummary=coverage?`目标收盘数据 ${coverage.completed}/${coverage.expected} 个交易对${coverage.missing.length?`，尚缺 ${coverage.missing.length} 个`:''}。`:'';
  s.getRange('A4').values=[[`${recordSummary}${coverageSummary}`]];
  if(book.refresh_reason){
   const reason={STARTUP:'启动快照（历史恢复中）',CANDLE_CLOSE:'收盘刷新',MANUAL:'手动刷新'}[book.refresh_reason]||book.refresh_reason;
   const boundary=book.refresh_boundary==null?'':`，对应收盘时间 ${new Date(book.refresh_boundary+28800000).toISOString().slice(0,19).replace('T',' ')}（北京时间）`;
   s.getRange('A5').values=[[`${reason}${boundary}`]];
  }
  s.getRangeByIndexes(5,0,1,columns.length).values=[columns.map(x=>x.label)];
  const header=s.getRangeByIndexes(5,0,1,columns.length);
  header.format.fill='#15344F';header.format.font={name:'Microsoft YaHei',bold:true,color:'#FFFFFF',size:10};
  header.format.rowHeight=38;header.format.wrapText=true;
  if(rows.length)s.getRangeByIndexes(6,0,rows.length,columns.length).values=rows.map(row=>columns.map(x=>{
   const value=row[x.key]??null;
   return x.type==='date'?serial(value):clean(value);
  }));
  columns.forEach((x,index)=>{
   s.getRangeByIndexes(0,index,last,1).format.columnWidth=x.width;
   const data=s.getRangeByIndexes(6,index,n,1);
   data.setNumberFormat(x.type==='date'?'yyyy-mm-dd hh:mm:ss':x.type==='price'?'0.00000000':x.type==='rate'?'0.00%':x.type==='integer'?'0':x.type==='number'?'#,##0.0000':'General');
   if(x.type==='text')data.format.wrapText=true;
  });
  if(key==='overview')rows.forEach((row,index)=>{
   const digits=Math.max(2,Math.min(12,row.price_precision??8));
   s.getRangeByIndexes(index+6,3,1,4).setNumberFormat('0.'+'0'.repeat(digits));
   if(row.health!=='OK'||row.readiness==='NOT_READY')s.getRangeByIndexes(index+6,18,1,1).format.fill='#FFF1D6';
  });
  if(key==='parameters')rows.forEach((row,index)=>{
   s.getRangeByIndexes(index+6,0,1,2).format.rowHeight=Math.max(42,Math.ceil(String(row.value).length/90)*19);
  });
  if(key==='status')s.getRangeByIndexes(6,0,n,columns.length).format.rowHeight=40;
  const table=s.tables.add(`A6:${col(columns.length-1)}${last}`,true,`T_${key}`);
  table.showFilterButton=true;
  s.getRangeByIndexes(5,0,n+1,columns.length).format.borders={preset:'all',style:'thin',color:'#B7C5D3'};
  s.freezePanes.freezeRows(6);
 }
 wb.recalculate();
 const errors=await wb.inspect({kind:'match',searchTerm:'#REF!|#DIV/0!|#VALUE!|#NAME\\?|#NUM!|#NULL!|#SPILL!|#CALC!',options:{useRegex:true,maxResults:20},maxChars:2000});
 const sample=await wb.inspect({kind:'table',range:'监控概览!A6:G9',include:'values,formulas',tableMaxRows:4,tableMaxCols:7,maxChars:2500});
 if(spec.render){
  for(const [key,name]of Object.entries(names)){
   const range=key==='parameters'?'A1:B11':key==='status'?'A1:E11':key==='triggers'||key==='previous'?'A1:F11':'A1:H12';
   const png=await wb.render({sheetName:name,range,scale:1.2,format:'png'});
   await fs.writeFile(path.join(out,`${book.interval}_${key}.png`),new Uint8Array(await png.arrayBuffer()));
  }
 }
 await (await SpreadsheetFile.exportXlsx(wb)).save(path.join(out,book.filename));
 verification.push({file:book.filename,interval:book.interval,rows:Object.fromEntries(Object.keys(schemas).map(k=>[k,book[k].length])),errors:errors.ndjson,sample:sample.ndjson});
}
await fs.writeFile(path.join(out,'artifact-verification.json'),JSON.stringify(verification,null,2));
console.log(JSON.stringify({exported:verification.length}));
