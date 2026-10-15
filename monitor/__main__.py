import argparse
import asyncio
import json
from pathlib import Path
from .service import Monitor, InstanceLock, load_config, health_snapshot
from .core import Store

def main():
    parser=argparse.ArgumentParser(description='币安USDT合约只读监控')
    parser.add_argument('command',choices=['init','bootstrap','run','export','status','stop'])
    parser.add_argument('--config',default='config.json')
    parser.add_argument('--seconds',type=int,default=0,help='run模式验证时长，0持续运行')
    args=parser.parse_args()
    config=load_config(args.config)
    if args.command=='stop':
        (Path(config['data_dir'])/'STOP').touch();print('已写入停止请求');return
    if args.command=='status':
        status=Path(config['data_dir'])/'health.json'
        print(status.read_text(encoding='utf-8') if status.exists() else '尚未运行');return
    with InstanceLock(config['data_dir']):
        if args.command in ('init','export'):
            store=Store(Path(config['data_dir'])/'monitor.sqlite3',config)
            try:
                if args.command=='export':
                    from .export import export_all
                    print(json.dumps(export_all(store,config),ensure_ascii=False))
                else: print('数据库初始化完成')
            finally: store.close()
            return
        (Path(config['data_dir'])/'STOP').unlink(missing_ok=True)
        monitor=Monitor(config)
        try: asyncio.run(monitor.run(args.command,args.seconds))
        except KeyboardInterrupt: print('收到停止信号；状态已保存，重启将补数')
        finally: monitor.store.close()

if __name__=='__main__': main()
