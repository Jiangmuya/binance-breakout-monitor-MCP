from .core import dumps
from .archive import ARCHIVE_DIR, period_label

LABELS={'15m':'15分钟','1h':'1小时','4h':'4小时','1d':'日线'}
SOURCE='https://fapi.binance.com/fapi/v1/klines'

def parameter_rows(store, config, interval, stamp, boundary, uni, missing, rules, report_start, report_end, count, versions, refresh_reason, *, not_applicable=None, current_warnings=None):
    not_applicable=list(not_applicable or [])
    current_warnings=list(current_warnings or [])
    expected=len(uni)-len(not_applicable)
    parameters=[{'key':'策略版本','value':store.version},{'key':'数据来源','value':SOURCE},
                {'key':'生成时间UTC毫秒','value':stamp},
                {'key':'刷新原因','value':refresh_reason},
                {'key':'本次对应收盘UTC毫秒','value':boundary},
                {'key':'本次收盘覆盖','value':f'已处理{expected-len(missing)}/{expected}；缺数/异常{len(missing)}；全部标的{len(uni)}；该收盘不适用{len(not_applicable)}'},
                {'key':'收盘适用范围','value':'上市时间不早于该收盘的交易对标注NOT_APPLICABLE，保留全部状态展示但不计入该收盘应处理数；未知上市时间仍计入应处理；不适用标的：'+('、'.join(not_applicable) or '无')},
                {'key':'已处理与当前健康','value':'已处理表示有效目标收盘判定已持久化，不表示后续行情冲突或连接异常已解决；当前异常仍保留在状态页。当前异常标的：'+('、'.join(current_warnings) or '无')},
                {'key':'自动刷新规则','value':'启动与对应收盘后刷新；缺数时明确标注不完整，补数完成后更新同一收盘批次；已完成历史期仅在记录变化时更新。'},
                {'key':'触发时间口径','value':'触发发现时间=程序判断并发现信号的时间；触发K线收盘时间=API收盘UTC毫秒+1；两者不等同，均以北京时间显示。'},
                {'key':'时间口径','value':'北京时间显示；UTC毫秒保留；API收盘时间=周期结束边界−1ms；日线北京08:00结束'},
                {'key':'EMA公式','value':'alpha=2/(N+1); EMA=alpha*C+(1-alpha)*上次EMA；首根收盘价种子；仅12/50/200'},
                {'key':'比较窗口','value':'当前策略为此前15根已收盘K线，不含目标K线；15分钟/1小时/4小时/日线各自独立。旧策略记录仍为20根，以各行“比较窗口根数”为准；排名包含目标K线，相同值并列。'},
                {'key':'监控范围','value':'仅USDT保证金、USDT报价且正在交易的永续合约；旧策略交割合约历史记录保留，不再新增监控。'},
                {'key':'EMA触发条件','value':'当前普通条件要求收盘价高于EMA12、EMA50和EMA200；不要求多头排列或EMA12上升。特别条件不要求EMA条件。“EMA多头排列”仅展示客观状态。'},
                {'key':'触发依据','value':'普通条件=六项普通条件同时满足且累计K线至少200根；特别条件=量比和实体扩张两项同时达标；普通条件＋特别条件=两条路径均满足。任一路径满足即可触发。同一根K线只产生一条信号；六项普通检查仍如实显示。'},
                {'key':'特别条件','value':f'15分钟、1小时、4小时、日线均适用：量比>= {rules.get("special_volume_multiple",12):g} 且实体扩张倍数>= {rules.get("special_body_multiple",6):g}；各用此前15根完整已收盘K线；均量与平均相对实体须大于零。特别条件不要求阳线、突破、EMA、涨幅扩张、实体/上影占比或V>V15_max；仅需目标K线加此前15根，不要求200根EMA预热。合法完整行情、永续交易范围和提醒时效规则仍保留。'},
                {'key':'采集窗口','value':'15分钟收盘前2分钟、1小时收盘前5分钟、4小时收盘前10分钟、日线收盘前15分钟开启实时订阅；同一收盘时刻涉及多个周期时统一采用最长提前量，例如北京08:00四周期均从07:45采集。只在收盘后判断和提醒；收齐后关闭该窗口，缺失数据定向补取；启动历史恢复仍保留。'},
                {'key':'放量与红字','value':f'当前{LABELS[interval]}普通放量条件：V>={rules["volume_multiple"]:g}×V15_mean 且 V>V15_max；V>={rules["volume_highlight_multiple"]:g}×V15_mean 且 V>V15_max整行红字。已保存为特别条件或普通＋特别同时触发的记录整行蓝字，蓝字优先于红字，不要求V>V15_max；触发记录和监控概览均适用。旧记录不按当前量比追溯判断特别条件；原放量红字判定保留。旧20根统计不按15根口径标红。'},
                {'key':'字段单位','value':'价格USDT；v基础币数量；q USDT成交额；收益率为小数；延迟毫秒；量比/实体比为倍数'},
                {'key':'计算口径','value':'统计由版本化监控引擎计算并持久化，Excel为只读快照；修改Excel不会改变监控参数'},
                {'key':'实体公式','value':'abs_body=abs(C-O); relative_body=abs(C/O-1); bar_return=C/O-1; close_return=C/前收盘-1'},
                {'key':'量价比较','value':'volume_ratio=V/比较均量; relative_body_ratio=relative_body/比较相对实体均值；比较窗口根数随原策略保留，不重算历史记录。'},
                {'key':'形态与突破','value':f'当前普通条件分母=abs(C-O)+H-max(O,C)，不含下影线；body_share=abs(C-O)/分母>={rules["min_body_share"]:.0%}，upper_wick_share=(H-max(O,C))/分母<={rules["max_upper_wick_share"]:.0%}；实体与上影占比之和为100%，两个边界均包含等号。breakout=C/比较最高价-1；特别条件不要求形态与突破。旧记录以原阈值为准。'},
                {'key':'旧策略记录','value':'旧20根记录的实体/上影占比仍以H-L为分母，EMA要求原多头排列及EMA12上升，放量阈值为原配置；原信号与统计不重算，详见策略版本及阈值。'},
                {'key':'提醒区别','value':'HISTORICAL初始化；RECOVERED补数；REALTIME实时；EXPIRED发现超过120秒。NOT_REQUESTED未要求发送'},
                {'key':'历史配置','value':dumps({'historical_analysis':config['historical_analysis'],'historical_summary':config['historical_summary']})},
                {'key':'记录归属期 北京','value':period_label(report_start,report_end)+'；含起始，不含结束'},
                {'key':'归属时间口径','value':'按触发发现时间discovered分期，08:00及以后发现的提醒进入新期；K线开收盘时间与事件类型保持原值。'},
                {'key':'归档说明','value':f'全部记录保留数据库；本文件{count}条信号；15分钟每日、1小时每周一、4小时以2026-10-19为起点每两周周一、日线每月1日，均北京时间08:00换期；历史文件在{ARCHIVE_DIR}；容量上限{config["excel_max_data_rows"]}行，超限按期内分卷。'},
                {'key':'注意','value':'仅监控条件触发，不代表实际交易或盈利预测；快照中的stale反映已收盘K线缺口。'},
                {'key':'配置','value':dumps(config)}]
    parameters += [{'key':f'历史策略 {v["version"]}', 'value':v['config']} for v in versions]
    return parameters
