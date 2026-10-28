// A bounded OOXML writer: rows, XML, compression, and disk writes all participate
// in stream backpressure. No workbook model or shared-string table is retained.
import fs from 'node:fs';
import fsp from 'node:fs/promises';
import path from 'node:path';
import readline from 'node:readline';
import {finished} from 'node:stream/promises';
import {PassThrough} from 'node:stream';
import {createHash} from 'node:crypto';
import {DatabaseSync} from 'node:sqlite';
import archiver from 'archiver';

export const XML_DECL='<?xml version="1.0" encoding="UTF-8" standalone="yes"?>';
const NS='http://schemas.openxmlformats.org/spreadsheetml/2006/main';
const REL='http://schemas.openxmlformats.org/officeDocument/2006/relationships';
const PKG='http://schemas.openxmlformats.org/package/2006/relationships';
export const xml=v=>String(v).replace(/[\u0000-\u0008\u000b\u000c\u000e-\u001f]/g,'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&apos;'}[c]));
export const col=n=>{let s='';for(n++;n;n=Math.floor((n-1)/26))s=String.fromCharCode(65+(n-1)%26)+s;return s;};
export const serial=ms=>ms==null?null:(ms+28800000)/86400000+25569;
export const sha=s=>createHash('sha256').update(s).digest('hex');
export const rowCount=source=>Array.isArray(source)?source.length:Number(source?.count??0);
export async function* readRows(source){
  if(Array.isArray(source)){yield*source;return;}
  if(source==null)return;
  if(!path.isAbsolute(source.ndjson??'')||!Number.isSafeInteger(source.count)||source.count<0)throw new Error('INVALID_ROW_SOURCE');
  const input=fs.createReadStream(source.ndjson,{highWaterMark:64*1024,encoding:'utf8'});
  const lines=readline.createInterface({input,crlfDelay:Infinity});
  let n=0;
  try{for await(const line of lines){if(!line.trim())continue;if(Buffer.byteLength(line)>4*1024*1024)throw new Error('ROW_INPUT_TOO_LARGE');yield JSON.parse(line);n++;}}
  finally{lines.close();input.destroy();}
  if(n!==source.count)throw new Error(`ROW_COUNT_MISMATCH: expected ${source.count}, read ${n}`);
}

const formats=['General','yyyy-mm-dd hh:mm:ss','0.00000000','0.00%','0','#,##0.0000',...Array.from({length:11},(_,i)=>'0.'+'0'.repeat(i+2))];
const uniqueFormats=[...new Set(formats)];
const formatIds=new Map(uniqueFormats.map((f,i)=>[f,f==='General'?0:163+i]));
const styles=[];const styleIds=new Map();
function addStyle(font,fill,border,format,wrap){const key=[font,fill,border,format,wrap].join('|');if(styleIds.has(key))return styleIds.get(key);const id=styles.length;styleIds.set(key,id);styles.push({font,fill,border,format,wrap});return id;}
addStyle(0,0,0,'General',false);
const titleStyle=addStyle(3,0,0,'General',false),headerStyle=addStyle(2,1,1,'General',true);
const dateStyle=addStyle(0,0,0,'yyyy-mm-dd hh:mm:ss',false);
for(const format of uniqueFormats)for(const font of [0,1])for(const fill of [0,2])for(const wrap of [false,true])addStyle(font,fill,1,format,wrap);
// Append blue styles so the original normal/red style indices stay stable.
for(const format of uniqueFormats)for(const fill of [0,2])for(const wrap of [false,true])addStyle(4,fill,1,format,wrap);
const formatFor=type=>({date:'yyyy-mm-dd hh:mm:ss',price:'0.00000000',rate:'0.00%',integer:'0',number:'#,##0.0000'}[type]??'General');
function styleXml(){
  const numfmts=uniqueFormats.filter(f=>f!=='General').map(f=>`<numFmt numFmtId="${formatIds.get(f)}" formatCode="${xml(f)}"/>`).join('');
  const fonts=[['FF22334A',10,false],['FFFF0000',10,false],['FFFFFFFF',10,true],['FF15344F',17,true],['FF0000FF',10,false]].map(([color,size,bold])=>`<font><sz val="${size}"/><color rgb="${color}"/><name val="Microsoft YaHei"/>${bold?'<b/>':''}</font>`).join('');
  // OOXML requires the first two fills to be none and gray125. Style fills below
  // use logical ids 0/1/2 mapped to physical 0/2/3.
  const xfs=styles.map(s=>`<xf numFmtId="${formatIds.get(s.format)}" fontId="${s.font}" fillId="${s.fill? s.fill+1:0}" borderId="${s.border}" xfId="0" applyFont="1" applyFill="1" applyBorder="1" applyNumberFormat="1" applyAlignment="1"><alignment vertical="center"${s.wrap?' wrapText="1"':''}/></xf>`).join('');
  const border='<border>'+['left','right','top','bottom'].map(edge=>`<${edge} style="thin"><color rgb="FFB7C5D3"/></${edge}>`).join('')+'<diagonal/></border>';
  return XML_DECL+`<styleSheet xmlns="${NS}"><numFmts count="${uniqueFormats.length-1}">${numfmts}</numFmts><fonts count="5">${fonts}</fonts><fills count="4"><fill><patternFill patternType="none"/></fill><fill><patternFill patternType="gray125"/></fill><fill><patternFill patternType="solid"><fgColor rgb="FF15344F"/><bgColor indexed="64"/></patternFill></fill><fill><patternFill patternType="solid"><fgColor rgb="FFFFF1D6"/><bgColor indexed="64"/></patternFill></fill></fills><borders count="2"><border/><${''}!-- data borders -->${border}</borders><cellStyleXfs count="1"><xf numFmtId="0" fontId="0" fillId="0" borderId="0"/></cellStyleXfs><cellXfs count="${styles.length}">${xfs}</cellXfs><cellStyles count="1"><cellStyle name="Normal" xfId="0" builtinId="0"/></cellStyles><dxfs count="0"/><tableStyles count="0" defaultTableStyle="TableStyleMedium2" defaultPivotStyle="PivotStyleLight16"/></styleSheet>`;
}
function cell(address,value,style=0){
  const start=`<c r="${address}" s="${style}"`;
  if(value==null)return start+'/>';
  if(typeof value==='number'){if(!Number.isFinite(value))throw new Error(`NON_FINITE_CELL: ${address}`);return start+`><v>${value}</v></c>`;}
  if(typeof value==='boolean')return start+` t="b"><v>${+value}</v></c>`;
  const text=String(value);if(text.length>32767)throw new Error(`CELL_TEXT_TOO_LONG: ${address}`);
  return start+` t="inlineStr"><is><t xml:space="preserve">${xml(text)}</t></is></c>`;
}
const clean=v=>typeof v==='string'&&/^[=+@]/.test(v)?"'"+v:v;
const periodTime=ms=>new Date(ms+28800000).toISOString().slice(0,19).replace('T',' ');
const relationship=(id,type,target)=>`<Relationship Id="${id}" Type="${REL}/${type}" Target="${xml(target)}"/>`;

export class FormatCache{
  constructor(filename,version){
    this.version=version;this.hits=0;this.misses=0;this.pending=0;
    this.db=new DatabaseSync(filename);
    // Keep large XML values in the append-oriented rowid table. A WITHOUT ROWID
    // table stores them beside random SHA keys and amplifies cold-cache writes.
    // Existing caches remain readable without any bulk migration or invalidation.
    this.db.exec('PRAGMA journal_mode=WAL; PRAGMA busy_timeout=30000; PRAGMA cache_size=-4096; PRAGMA synchronous=NORMAL; CREATE TABLE IF NOT EXISTS formatted_rows(hash TEXT PRIMARY KEY,xml TEXT NOT NULL,height INTEGER NOT NULL);');
    this.get=this.db.prepare('SELECT xml,height FROM formatted_rows WHERE hash=?');
    this.put=this.db.prepare('INSERT OR IGNORE INTO formatted_rows(hash,xml,height) VALUES(?,?,?)');
  }
  prepare(source,key,book,columns,normalize){
    const hash=sha(this.version+'\0'+key+'\0'+String(book.comparison_window??20)+'\0'+JSON.stringify(source));
    const cached=this.get.get(hash);if(cached){this.hits++;return cached;}
    this.misses++;const row=normalize(source,book);const signalSheet=['overview','triggers'].includes(key);
    // Use the persisted trigger route; never infer special signals from old ratios.
    const blue=signalSheet&&(row.trigger_path==='SPECIAL'||row.trigger_path==='BOTH'||
      (row.trigger_path==null&&row.special_trigger===true));
    const red=signalSheet&&row.volume_highlight;
    let body='';
    for(let i=0;i<columns.length;i++){
      const c=columns[i];let format=formatFor(c.type);
      if(key==='overview'&&i>=3&&i<=6)format='0.'+'0'.repeat(Math.max(2,Math.min(12,row.price_precision??8)));
      const warning=key==='overview'&&i===18&&(row.health!=='OK'||row.readiness==='NOT_READY');
      const style=styleIds.get([blue?4:red?1:0,warning?2:0,1,format,c.type==='text'].join('|'));
      const value=row[c.key]??null;body+=cell(`${col(i)}0`,c.type==='date'?serial(value):clean(value),style);
    }
    const height=key==='parameters'?Math.max(42,Math.ceil(String(row.value).length/90)*19):['status','recent_anomalies'].includes(key)?40:23;
    if(!this.pending)this.db.exec('BEGIN');this.put.run(hash,body,height);this.pending++;
    if(this.pending>=5000)this.flush();return {xml:body,height};
  }
  flush(){if(this.pending){this.db.exec('COMMIT');this.pending=0;}}
  close(){this.flush();this.db.close();}
}

async function write(stream,value){
  if(stream.destroyed||stream.writableEnded)throw stream.errored??new Error('OUTPUT_STREAM_CLOSED');
  if(stream.write(value))return;
  if(stream.destroyed)throw stream.errored??new Error('OUTPUT_STREAM_CLOSED');
  await new Promise((resolve,reject)=>{
    const cleanup=()=>{stream.off('drain',drain);stream.off('error',error);stream.off('close',close);};
    const drain=()=>{cleanup();resolve();};
    const error=cause=>{cleanup();reject(cause);};
    const close=()=>error(stream.errored??new Error('OUTPUT_STREAM_CLOSED'));
    stream.once('drain',drain);stream.once('error',error);stream.once('close',close);
  });
}
export async function writeWorkbook(book,destination,{schemas,names,sheetOrder,normalize,cache,sample,batchRows=5000,batchBytes=50*1024*1024}){
  const output=fs.createWriteStream(destination,{highWaterMark:64*1024});
  const zip=archiver('zip',{zlib:{level:6},highWaterMark:64*1024});
  let currentStream;let failure;
  const fail=error=>{failure??=error;currentStream?.destroy(failure);zip.abort();output.destroy(failure);};
  zip.on('error',fail);zip.on('warning',fail);output.on('error',fail);
  const completion=finished(output);completion.catch(()=>{});zip.pipe(output);
  const append=(name,body)=>{if(failure)throw failure;zip.append(body,{name,date:new Date(2000,0,1)});};
  const counts={};const idHash=createHash('sha256');let signalCount=0;
  try{
    append('_rels/.rels',XML_DECL+`<Relationships xmlns="${PKG}">${relationship('rId1','officeDocument','xl/workbook.xml')}</Relationships>`);
    append('xl/workbook.xml',XML_DECL+`<workbook xmlns="${NS}" xmlns:r="${REL}"><bookViews><workbookView activeTab="0" firstSheet="0"/></bookViews><sheets>${sheetOrder.map((k,i)=>`<sheet name="${xml(names[k])}" sheetId="${i+1}" r:id="rId${i+1}"/>`).join('')}</sheets><calcPr calcId="0" fullCalcOnLoad="0"/></workbook>`);
    append('xl/_rels/workbook.xml.rels',XML_DECL+`<Relationships xmlns="${PKG}">${sheetOrder.map((_,i)=>relationship(`rId${i+1}`,'worksheet',`worksheets/sheet${i+1}.xml`)).join('')}${relationship(`rId${sheetOrder.length+1}`,'styles','styles.xml')}</Relationships>`);
    append('xl/styles.xml',styleXml());
    const types=[['/xl/workbook.xml','spreadsheetml.sheet.main'],['/xl/styles.xml','spreadsheetml.styles'],...sheetOrder.flatMap((_,i)=>[[`/xl/worksheets/sheet${i+1}.xml`,'spreadsheetml.worksheet'],[`/xl/tables/table${i+1}.xml`,'spreadsheetml.table']])];
    append('[Content_Types].xml',XML_DECL+'<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types"><Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/><Default Extension="xml" ContentType="application/xml"/>'+types.map(([p,t])=>`<Override PartName="${p}" ContentType="application/vnd.openxmlformats-officedocument.${t}+xml"/>`).join('')+'</Types>');
    for(let sheetIndex=0;sheetIndex<sheetOrder.length;sheetIndex++){
      const key=sheetOrder[sheetIndex],columns=schemas[key],expected=rowCount(book[key]),last=6+Math.max(1,expected);
      if(last>1048576)throw new Error(`EXCEL_ROW_LIMIT: ${key}`);
      const range=`A6:${col(columns.length-1)}${last}`;
      append(`xl/tables/table${sheetIndex+1}.xml`,XML_DECL+`<table xmlns="${NS}" id="${sheetIndex+1}" name="T_${key}" displayName="T_${key}" ref="${range}" totalsRowShown="0"><autoFilter ref="${range}"/><tableColumns count="${columns.length}">${columns.map((c,i)=>`<tableColumn id="${i+1}" name="${xml(c.label)}"/>`).join('')}</tableColumns><tableStyleInfo name="TableStyleMedium2" showFirstColumn="0" showLastColumn="0" showRowStripes="0" showColumnStripes="0"/></table>`);
      append(`xl/worksheets/_rels/sheet${sheetIndex+1}.xml.rels`,XML_DECL+`<Relationships xmlns="${PKG}">${relationship('rId1','table',`../tables/table${sheetIndex+1}.xml`)}</Relationships>`);
      currentStream=new PassThrough({highWaterMark:64*1024});currentStream.on('error',()=>{});append(`xl/worksheets/sheet${sheetIndex+1}.xml`,currentStream);
      await write(currentStream,XML_DECL+`<worksheet xmlns="${NS}" xmlns:r="${REL}"><dimension ref="A1:${col(columns.length-1)}${last}"/><sheetViews><sheetView workbookViewId="0" showGridLines="0"${sheetIndex===0?' tabSelected="1"':''}><pane ySplit="6" topLeftCell="A7" activePane="bottomLeft" state="frozen"/><selection pane="bottomLeft" activeCell="A7" sqref="A7"/></sheetView></sheetViews><sheetFormatPr defaultRowHeight="23"/><cols>${columns.map((c,i)=>`<col min="${i+1}" max="${i+1}" width="${c.width}" customWidth="1"/>`).join('')}</cols><sheetData>`);
      const period=book.report_period_start==null||book.report_period_end==null?'':`本期按提醒发现时间归属：${periodTime(book.report_period_start).slice(0,16)} 至 ${periodTime(book.report_period_end).slice(0,16)}（北京时间，含起不含止）。`;
      const cv=book.coverage;const missing=Math.max(cv?.missing?.length??0,(cv?.expected??0)-(cv?.completed??0));
      const completeness=!(cv?.expected>0)?'不完整（应处理交易对数量尚未就绪）':missing?'不完整（尚缺 '+missing+' 个）':'完整';
      const coverage=cv?`目标收盘数据 ${cv.completed}/${cv.expected} 个交易对；${completeness}。`:'';
      let summary=expected?`本表记录 ${expected} 条；数值来自持久化数据库。`:'本次无记录；未填入任何模拟信号。';
      if(key==='recent_anomalies')summary+=book.recent_anomalies_description??'最近24小时，最多2000条异常分组；显示首次时间、最近时间、发生次数。完整异常历史保留在数据库及增量档案。';
      const reason={STARTUP:'启动快照（历史恢复中）',CANDLE_CLOSE:'收盘刷新',MANUAL:'手动刷新',CANDLE_REPAIR:'同一收盘批次补数刷新'}[book.refresh_reason]??book.refresh_reason??'';
      const boundary=book.refresh_boundary==null?'':`，对应收盘时间 ${periodTime(book.refresh_boundary)}（北京时间）`;
      await write(currentStream,`<row r="1" ht="34" customHeight="1">${cell('A1',`${names[key]} · ${book.interval}`,titleStyle)}</row><row r="2">${cell('A2','只读监控；真实行情快照。条件触发不代表实际交易。'+period)}</row><row r="3">${cell('A3','快照时间 北京')}${cell('B3',serial(book.generated),dateStyle)}</row><row r="4">${cell('A4',summary+coverage)}</row><row r="5">${cell('A5',reason+boundary)}</row><row r="6" ht="38" customHeight="1">${columns.map((c,i)=>cell(`${col(i)}6`,c.label,headerStyle)).join('')}</row>`);
      let n=0,chunkRows=0,chunkBytes=0;
      for await(const source of readRows(book[key])){
        const prepared=cache.prepare(source,key,book,columns,normalize),rowNumber=7+n;
        const body=prepared.xml.replace(/(<c r="[A-Z]+)0(" )/g,`$1${rowNumber}$2`);
        const rowXml=`<row r="${rowNumber}" ht="${prepared.height}" customHeight="1">${body}</row>`;
        await write(currentStream,rowXml);n++;chunkRows++;chunkBytes+=Buffer.byteLength(rowXml);
        if(key==='triggers'){idHash.update(String(source.signal_id??'')+'\n');signalCount++;}
        if(chunkRows>=batchRows||chunkBytes>=batchBytes){cache.flush();sample();chunkRows=0;chunkBytes=0;await new Promise(resolve=>setImmediate(resolve));}
      }
      if(n!==expected)throw new Error(`ROW_COUNT_MISMATCH ${key}: ${n}/${expected}`);
      if(!n){const blank=cache.prepare({},key,book,columns,normalize);await write(currentStream,`<row r="7" ht="23" customHeight="1">${blank.xml.replace(/(<c r="[A-Z]+)0(" )/g,'$17$2')}</row>`);}
      counts[key]=n;
      // The table owns its filter; a worksheet filter cannot overlap the table.
      await write(currentStream,'</sheetData><pageMargins left="0.7" right="0.7" top="0.75" bottom="0.75" header="0.3" footer="0.3"/><tableParts count="1"><tablePart r:id="rId1"/></tableParts></worksheet>');
      currentStream.end();sample();
    }
    cache.flush();await Promise.race([zip.finalize(),completion]);await completion;if(failure)throw failure;
    return {filename:path.basename(destination),counts,signal_count:signalCount,signal_id_sha256:idHash.digest('hex'),bytes_written:zip.pointer()};
  }catch(error){failure??=error;zip.abort();currentStream?.destroy();output.destroy();await completion.catch(()=>{});await fsp.rm(destination,{force:true}).catch(()=>{});throw failure;}
}
