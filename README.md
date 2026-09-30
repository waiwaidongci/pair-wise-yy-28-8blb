# 影视拍摄连续性管理

项目使用 Python 标准库、SQLite 和 `http.server` 管理非线性拍摄中的连续性。场次与镜头分别记录叙事顺序和拍摄顺序，角色、服装、道具、伤痕状态按叙事链检查，冲突可由调整方案或正式豁免处理，镜头只有在无未处理冲突时才能锁定。

## 运行与测试

```bash
python app.py
python -m unittest discover -s tests -v
```

默认端口 `8115`，页面 <http://127.0.0.1:8115>。首次启动创建“雨夜追踪”示例，其中拍摄顺序与叙事顺序相反，并生成一个伤痕回退冲突。数据库和端口可分别用 `CONTINUITY_DB`、`PORT` 指定。

## 连续性算法

每个元素选择一种规则：

- `stable`：沿叙事顺序状态必须一致。
- `monotonic`：使用 `numeric_value` 比较，数值不能下降，适合伤痕、污损或破坏程度。
- `allowed`：只有预先登记的状态转移才能通过。

检测按叙事顺序执行，与剪辑和拍摄顺序无关。调整方案必须由制片人或场记提出、由另一位审片人批准；批准后写入镜头状态并重新检查。也可以为确实需要保留的冲突写入豁免理由。锁定会再次检查场次，豁免之外的活跃冲突会阻止锁定，锁定后直接改状态会失败。

## 主要接口

- `POST /api/users`、`POST /api/productions`
- `POST /api/productions/{id}/scenes`、`POST /api/scenes/{id}/shots`
- `POST /api/productions/{id}/elements`、`POST /api/elements/{id}/transitions`
- `POST /api/shots/{id}/states`、`POST /api/scenes/{id}/check`
- `POST /api/shots/{id}/reorder` — 调整未锁定镜头的叙事顺序，按新顺序重算冲突
- `POST /api/conflicts/{id}/plans`、`POST /api/plans/{id}/review`
- `POST /api/conflicts/{id}/exemptions`
- `POST /api/shots/{id}/lock`
- `GET /api/productions/{id}/continuity`

## 并发提交与重试

所有写入接口（`states`、`reorder`、`lock`）都支持两个字段：

- `version`：场记打开编辑时镜头的版本号。提交时若与当前版本不一致，返回 `409` 并提示版本冲突，后到的终端需刷新后基于最新版本重试。两个终端同时提交同一场次时，先到的一次生效，后到的收到版本冲突。
- `request_id`：请求编号。写入失败后重试沿用同一编号，服务端幂等回放已成功的写入，不会重复应用（版本号只增一次）；失败的写入整体回滚，场次、镜头、元素状态和冲突不留半套变化。

调序会按新叙事顺序重算受影响元素的冲突，旧冲突对应的调整方案随即标记为 `voided`（失效），此前的锁定结论也会失效——镜头解锁，需重新检查后再锁定。页面会显示当前叙事顺序、失效的方案和重试结果。
