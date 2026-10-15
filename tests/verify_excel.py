"""Read exported OOXML without Excel-authoring libraries; reconcile with export input."""
import json
import math
import sys
import zipfile
import xml.etree.ElementTree as ET
from pathlib import Path

NS={'s':'http://schemas.openxmlformats.org/spreadsheetml/2006/main'}

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
            assert [s.attrib['name'] for s in sheets]==['监控概览','触发记录','前20根对照','数据状态与异常','参数与说明']
            styles=ET.fromstring(z.read('xl/styles.xml'))
            formats=[n.attrib.get('formatCode','') for n in styles.findall('s:numFmts/s:numFmt',NS)]
            assert any('yyyy-mm-dd' in f for f in formats)
            assert any('%' in f for f in formats)
            assert any('0.0000' in f for f in formats)
            checked=0
            for index,key in enumerate(('overview','triggers','previous','status','parameters'),1):
                root,data=cells(z,f'xl/worksheets/sheet{index}.xml',strings)
                assert root.find('s:sheetViews/s:sheetView/s:pane',NS) is not None
                assert root.find('s:tableParts',NS) is not None
                if key=='overview':
                    for rownum,source in enumerate(book[key],7):
                        assert data.get(f'A{rownum}')==source['symbol']
                        for column,field in [('D','c'),('E','EMA12'),('F','EMA50'),('G','EMA200')]:
                            expected=source.get(field);actual=data.get(f'{column}{rownum}')
                            if expected is None:assert actual is None
                            else:assert math.isclose(actual,expected,rel_tol=1e-12,abs_tol=1e-12)
                            checked+=1
                        boundary=source.get('close_boundary')
                        if boundary:
                            expected=(boundary+28800000)/86400000+25569
                            assert math.isclose(data[f'C{rownum}'],expected,abs_tol=1e-9)
                elif key=='triggers':
                    assert {data.get(f'A{n}') for n in range(7,7+len(book[key]))}=={r['signal_id'] for r in book[key]}
                elif key=='previous':
                    assert len(book[key])==20*len(book['triggers'])
                    for n,row in enumerate(book[key],7):
                        assert data[f'A{n}']==row['signal_id']
                        assert data[f'D{n}']==row['index']
            records.append({'file':book['filename'],'interval':book['interval'],'overview_rows':len(book['overview']),
                            'signals':len(book['triggers']),'comparison_rows':len(book['previous']),
                            'numeric_values_checked':checked,'xlsx_reopened':True,'styles_filters_freeze':True,
                            'macros_external_links':False})
    return records

if __name__=='__main__':
    records=verify(sys.argv[1],sys.argv[2]);print(json.dumps(records,ensure_ascii=False,indent=2))
