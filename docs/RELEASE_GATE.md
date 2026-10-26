> 2026-10-26用户已结束持续验收并要求继续交付。下述24小时门禁仍保留原标准，不能给旧报告改成PASSED。本次另行记录用户结束验收、已知时效问题、固定快照对账和受控切换结果。

# 正式切换前的只读门禁

`tools/check_release.py` 只读取候选代码、影子运行证据与验收报告，不停止进程、不修改正式目录、不切换版本。返回 `READY_FOR_FINAL_FREEZE` 仅表示可以进入最终冻结对账；`ready_for_cutover` 始终为 `false`。

在候选程序目录执行，路径替换为实际验收位置：

```powershell
python .\tools\check_release.py --shadow 'D:\validation\shadow-72h' --static-acceptance 'D:\validation\real-backfill\acceptance.json' --load-acceptance 'D:\validation\load-10000\acceptance.json' --load-acceptance 'D:\validation\load-30000\acceptance.json'
```

退出码 `0` 表示进入最终冻结对账的前置检查通过；退出码 `2` 表示仍被阻止。JSON 中的 `blocking_checks` 和 `remaining_checks` 给出原因及剩余动作。

门禁要求同一代码版本真实连续运行至少 24 小时，并重新计算 `soak-state.json` 的结果，核对原始采样间隔、系统当前时间及操作系统记录的进程启动时间。仅修改报告中的 `PASSED` 或 `continuous_hours` 无法满足要求。默认父/子进程内存预算 512 MiB，收盘完整发布目标 180 秒；健康信息不超过 30 秒。采样默认为 5 秒，只有实际使用外部 60 秒观察器时才相应设置 `--sample-seconds 60`。

所有周期必须有完整的最近已到期收盘，数据库中不得有未完成批次、脏历史期、待替换文件或归档维护错误。静态与放大负载报告必须有完整记录对账、实际内存与耗时数据，并写入**执行验收当时**的 `code_hash`。旧报告缺少代码指纹时需要在冻结版本上重新验收，不得给旧报告倒填当前指纹。放大负载至少包含 10,000 个信号，且不小于静态样本的两倍。

运行中的 SQLite 与 Excel 会在不同时间更新，门禁不会把它们当作同一最终快照。前置检查通过后，还必须干净停止影子生产者并持有其 `instance.lock`；在该锁持续持有期间，使用 `tools/export_soak.py reconcile` 对冻结数据库及 `canonical-files.json` 中的全部文件做最终信号编号对账，确保遗漏、额外记录和重复编号均为零。之后另行执行受控切换，验证正式启动、收盘覆盖率与回退条件。

这些证据是本机文件与操作系统信息的交叉校验，不属于密码学签名证明。单元测试使用合成的 24 小时数据，只验证门禁逻辑，不能替代真实运行验收。
