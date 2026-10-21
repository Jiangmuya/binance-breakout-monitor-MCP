"""Read exported OOXML without Excel-authoring libraries; reconcile with export input."""
import json
from collections import Counter
import datetime as dt
import math
import posixpath
import re
import sys
import zipfile
import xml.etree.ElementTree as ET
from pathlib import Path

NS={'s':'http://schemas.openxmlformats.org/spreadsheetml/2006/main'}
REL_NS='http://schemas.openxmlformats.org/officeDocument/2006/relationships'
PACKAGE_REL_NS='http://schemas.openxmlformats.org/package/2006/relationships'
SHEETS=[('triggers','触发记录'),('overview','监控概览'),('previous','历史K线对照'),
        ('status','数据状态与异常'),('parameters','参数与说明')]
TRIGGER_HEADERS=[
    '交易对','合约类型','周期','开盘时间 北京','触发K线收盘时间 北京','开盘UTC毫秒','API收盘UTC毫秒',
    '开盘价','最高价','最低价','收盘价','基础币成交量','USDT成交额','EMA12','EMA50','EMA200',
    '比较均量','比较中位量','比较最大量','量比','有符号实体','绝对实体','相对实体',
    '比较相对实体均值','相对实体倍数','比较实体均值','比较实体中位','比较实体最大',
    '实体/均值','实体/中位','实体/最大','实体减均值','实体减中位','实体减最大','比较阳线数','阳线实体均值',
    '开收盘涨幅','相对前收盘涨幅','比较平均涨幅','比较中位涨幅','比较最大涨幅','涨幅减均值','涨幅减最大',
    '比较最高价','突破幅度','实体占比','上影占比','成交量排名','实体排名','涨幅排名',
    '阳线突破','EMA条件','放量','实体扩张','涨幅扩张','阳线形态','事件类型','数据来源阶段','预热状态',
    'EMA种子UTC毫秒','累计K线数','触发发现时间 北京','发现UTC毫秒','接收UTC毫秒','处理UTC毫秒','交易所事件UTC毫秒',
    '通知状态','发送时间 北京','发送UTC毫秒','发现延迟毫秒','发送延迟毫秒','通知尝试次数','阈值JSON',
    '比较窗口根数','signal_id','策略版本']

def window_of(row,book):
    return row.get('comparison_window',
        15 if 'V15_mean' in row else 20 if 'V20_mean' in row else book.get('comparison_window',20))

def is_high_volume(row,book):
    window=window_of(row,book)
    if window!=15:
        return False
    if isinstance(row.get('volume_highlight'),bool):
        return row['volume_highlight']
    mean=row.get('V_mean',row.get(f'V{window}_mean'))
    maximum=row.get('V_max',row.get(f'V{window}_max'))
    multiple=row.get('volume_highlight_multiple',5)
    return (mean is not None and maximum is not None
            and row.get('v',0)>=multiple*mean and row.get('v',0)>maximum)

def column(number):
    result=''
    while number:
        number,remainder=divmod(number-1,26)
        result=chr(65+remainder)+result
    return result

def relationships(archive,part):
    folder,filename=posixpath.split(part)
    relpath=posixpath.join(folder,'_rels',filename+'.rels')
    root=ET.fromstring(archive.read(relpath))
    result={}
    for rel in root.findall(f'{{{PACKAGE_REL_NS}}}Relationship'):
        assert rel.attrib.get('TargetMode')!='External', rel.attrib
        target=rel.attrib['Target']
        result[rel.attrib['Id']]=(target.lstrip('/') if target.startswith('/')
                                else posixpath.normpath(posixpath.join(folder,target)))
    return result

def numeric_date(data,cell_nodes,cell_xfs,number_formats,address,milliseconds):
    actual=data.get(address)
    assert isinstance(actual,(int,float)) and not isinstance(actual,bool), (address,actual)
    node=cell_nodes[address]
    assert node.attrib.get('t','n')=='n', (address,node.attrib)
    fmt_id=int(cell_xfs[int(node.attrib.get('s','0'))].attrib.get('numFmtId','0'))
    fmt=number_formats.get(fmt_id,'').lower()
    assert all(token in fmt for token in ('yyyy','mm','dd','hh','ss')), (address,fmt)
    expected=(milliseconds+28800000)/86400000+25569
    assert math.isclose(actual,expected,rel_tol=0,abs_tol=1e-9), (address,actual,expected)

def cells(archive,name,strings):
    root=ET.fromstring(archive.read(name));result={}
    for cell in root.findall('.//s:sheetData/s:row/s:c',NS):
        kind=cell.attrib.get('t');raw=cell.find('s:v',NS)
        if kind=='inlineStr':value=''.join(cell.itertext())
        elif raw is None:value=None
        elif kind=='s':value=strings[int(raw.text)]
        elif kind=='b':value=raw.text=='1'
        elif kind=='e':raise AssertionError(f'Excel error {raw.text}')
        else:
            try:value=float(raw.text)
            except ValueError:value=raw.text
        result[cell.attrib['r']]=value
    return root,result

def verify(directory,spec_path):
    spec=json.loads(Path(spec_path).read_text(encoding='utf-8'))
    records=[]
    for book in spec['books']:
        path=Path(directory)/book['filename']
        with zipfile.ZipFile(path) as z:
            assert z.testzip() is None
            assert not any('vbaProject' in n or 'externalLinks/' in n for n in z.namelist())
            strings=[]
            if 'xl/sharedStrings.xml' in z.namelist():
                strings=[''.join(e.itertext()) for e in ET.fromstring(z.read('xl/sharedStrings.xml')).findall('s:si',NS)]
            workbook=ET.fromstring(z.read('xl/workbook.xml'))
            sheets=workbook.findall('s:sheets/s:sheet',NS)
            assert [s.attrib['name'] for s in sheets]==[name for _,name in SHEETS]
            # OOXML's omitted activeTab defaults to zero, as does an omitted bookViews element.
            for view in workbook.findall('s:bookViews/s:workbookView',NS):
                assert int(view.attrib.get('activeTab','0'))==0, view.attrib
                assert int(view.attrib.get('firstSheet','0'))==0, view.attrib
            sheet_parts=relationships(z,'xl/workbook.xml')
            styles=ET.fromstring(z.read('xl/styles.xml'))
            formats=[n.attrib.get('formatCode','') for n in styles.findall('s:numFmts/s:numFmt',NS)]
            number_formats={int(n.attrib['numFmtId']):n.attrib.get('formatCode','')
                            for n in styles.findall('s:numFmts/s:numFmt',NS)}
            cell_xfs=styles.findall('s:cellXfs/s:xf',NS)
            fonts=styles.findall('s:fonts/s:font',NS)
            borders=styles.findall('s:borders/s:border',NS)
            assert any('yyyy-mm-dd' in f for f in formats)
            assert any('%' in f for f in formats)
            assert any('0.0000' in f for f in formats)
            checked=0; bordered_cells=0; verified_tables=set(); trigger_dates_checked=0; red_rows=0
            for sheet,(key,name) in zip(sheets,SHEETS):
                part=sheet_parts[sheet.attrib[f'{{{REL_NS}}}id']]
                root,data=cells(z,part,strings)
                cell_nodes={cell.attrib['r']:cell for cell in root.findall('.//s:sheetData/s:row/s:c',NS)}
                for view in root.findall('s:sheetViews/s:sheetView',NS):
                    if key!='triggers':assert view.attrib.get('tabSelected','0') not in ('1','true')
                pane=root.find('s:sheetViews/s:sheetView/s:pane',NS)
                assert pane is not None
                assert pane.attrib.get('state')=='frozen', (name,pane.attrib)
                assert float(pane.attrib.get('ySplit','0'))==6, (name,pane.attrib)
                assert float(pane.attrib.get('xSplit','0'))==0, (name,pane.attrib)
                table_parts=root.findall('s:tableParts/s:tablePart',NS)
                assert len(table_parts)==1, name
                sheet_rels=relationships(z,part)
                table_path=sheet_rels[table_parts[0].attrib[f'{{{REL_NS}}}id']]
                verified_tables.add(table_path)
                table=ET.fromstring(z.read(table_path))
                columns=table.findall('s:tableColumns/s:tableColumn',NS)
                last=6+max(1,len(book[key]))
                expected_ref=f'A6:{column(len(columns))}{last}'
                assert table.attrib['ref']==expected_ref, (name,table.attrib['ref'],expected_ref)
                autofilter=table.find('s:autoFilter',NS)
                assert autofilter is not None and autofilter.attrib['ref']==expected_ref, name
                assert all(f.attrib.get('hiddenButton','0') not in ('1','true')
                           for f in autofilter.findall('s:filterColumn',NS)), name
                headers=[data.get(f'{column(i)}6') for i in range(1,len(columns)+1)]
                assert headers==[c.attrib['name'] for c in columns], (name,headers)
                header_columns={label:column(index) for index,label in enumerate(headers,1)}
                for rownum in range(6,last+1):
                    for index in range(1,len(columns)+1):
                        address=f'{column(index)}{rownum}'
                        assert address in cell_nodes, (name,address,'missing bordered cell')
                        node=cell_nodes[address]
                        xf=cell_xfs[int(node.attrib.get('s','0'))]
                        border=borders[int(xf.attrib.get('borderId','0'))]
                        for edge in ('left','right','top','bottom'):
                            line=border.find(f's:{edge}',NS)
                            assert line is not None and line.attrib.get('style')=='thin', (name,address,edge)
                        bordered_cells+=1
                numeric_date(data,cell_nodes,cell_xfs,number_formats,'B3',book['generated'])
                if book.get('report_period_start') is not None and book.get('report_period_end') is not None:
                    fmt=lambda ms:dt.datetime.fromtimestamp(ms/1000,dt.timezone(dt.timedelta(hours=8))).strftime('%Y-%m-%d %H:%M')
                    assert (f'本期按提醒发现时间归属：{fmt(book["report_period_start"])} 至 '
                            f'{fmt(book["report_period_end"])}（北京时间，含起不含止）。') in data.get('A2','')
                if key in ('overview','triggers'):
                    for rownum,source in enumerate(book[key],7):
                        highlight=is_high_volume(source,book)
                        if highlight: red_rows+=1
                        for index in range(1,len(columns)+1):
                            node=cell_nodes[f'{column(index)}{rownum}']
                            xf=cell_xfs[int(node.attrib.get('s','0'))]
                            font=fonts[int(xf.attrib.get('fontId','0'))]
                            color=font.find('s:color',NS)
                            rgb='' if color is None else color.attrib.get('rgb','').upper()
                            assert rgb.endswith('FF0000')==highlight, (name,rownum,index,rgb,highlight)
                        assert data.get(f'{header_columns["比较窗口根数"]}{rownum}')==window_of(source,book)
                if key=='overview':
                    for rownum,source in enumerate(book[key],7):
                        assert data.get(f'A{rownum}')==source['symbol']
                        for colname,field in [('D','c'),('E','EMA12'),('F','EMA50'),('G','EMA200')]:
                            expected=source.get(field);actual=data.get(f'{colname}{rownum}')
                            if expected is None:assert actual is None
                            else:assert math.isclose(actual,expected,rel_tol=1e-12,abs_tol=1e-12)
                            checked+=1
                        boundary=source.get('close_boundary')
                        if boundary:
                            expected=(boundary+28800000)/86400000+25569
                            assert math.isclose(data[f'C{rownum}'],expected,rel_tol=0,abs_tol=1e-9)
                elif key=='triggers':
                    assert headers==TRIGGER_HEADERS, headers
                    assert header_columns['触发K线收盘时间 北京']=='E'
                    assert header_columns['触发发现时间 北京']=='BJ'
                    assert headers[-2:]==['signal_id','策略版本']
                    for rownum,source in enumerate(book[key],7):
                        value=lambda label:data.get(f'{header_columns[label]}{rownum}')
                        assert value('signal_id')==source['signal_id']
                        assert value('策略版本')==source['strategy_version']
                        assert value('交易对')==source['symbol']
                        window=window_of(source,book)
                        for label,field in [('比较均量','V_mean'),('比较最大量','V_max'),
                                            ('比较最高价','H'),('实体占比','body_share'),
                                            ('上影占比','upper_wick_share')]:
                            legacy=f'H{window}' if field=='H' else field.replace('_',f'{window}_',1)
                            expected=source.get(field,source.get(legacy));actual=value(label)
                            if expected is None: assert actual is None, (label,actual)
                            else: assert math.isclose(actual,expected,rel_tol=1e-12,abs_tol=1e-12)
                        for label,field,utc_label,adjustment in (
                            ('开盘时间 北京','t','开盘UTC毫秒',0),
                            ('触发K线收盘时间 北京','close_boundary','API收盘UTC毫秒',1),
                            ('触发发现时间 北京','discovered','发现UTC毫秒',0)):
                            milliseconds=source[field]
                            numeric_date(data,cell_nodes,cell_xfs,number_formats,
                                         f'{header_columns[label]}{rownum}',milliseconds)
                            utc=value(utc_label)
                            assert isinstance(utc,(int,float)) and not isinstance(utc,bool)
                            assert utc+adjustment==milliseconds, (source['signal_id'],label,utc,milliseconds)
                            trigger_dates_checked+=1
                elif key=='previous':
                    counts=Counter(row['signal_id'] for row in book[key])
                    assert counts==Counter({r['signal_id']:window_of(r,book) for r in book['triggers']})
                    for n,row in enumerate(book[key],7):
                        assert data[f'A{n}']==row['signal_id']
                        assert data[f'D{n}']==row['index']
                        assert data.get(f'{header_columns["比较窗口根数"]}{n}')==window_of(row,book)
            assert len(verified_tables)==5
            assert len([n for n in z.namelist() if re.fullmatch(r'xl/tables/[^/]+\.xml',n)])==5
            records.append({'file':book['filename'],'interval':book['interval'],'overview_rows':len(book['overview']),
                            'signals':len(book['triggers']),'comparison_rows':len(book['previous']),
                            'numeric_values_checked':checked,'xlsx_reopened':True,'styles_filters_freeze':True,
                            'first_active_sheet':'触发记录','trigger_column_order_verified':True,
                            'trigger_dates_checked':trigger_dates_checked,'all_borders_cells_checked':bordered_cells,
                            'high_volume_red_rows_checked':red_rows,
                            'macros_external_links':False})
    return records

if __name__=='__main__':
    records=verify(sys.argv[1],sys.argv[2]);print(json.dumps(records,ensure_ascii=False,indent=2))
