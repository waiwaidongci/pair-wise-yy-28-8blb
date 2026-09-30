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

### 非线性调整：版本号、失效与幂等重试

非线性拍摄时场记会在现场临时挪动未锁定镜头的叙事位置，`POST /api/shots/{id}/reorder` 处理这类提交：

- 请求体必须带 `expected_version`（场记打开编辑时看到的镜头版本号）和 `request_id`（请求编号）。
- 同一场景两个终端同时提交时，`BEGIN IMMEDIATE` 串行化写入：先到的提交生效并把场景内镜头版本号 +1，后到的提交版本号已过期，返回 **HTTP 409 版本冲突**，提示打开版本/当前版本且不写入任何变化。
- 位置变化后按**新叙事顺序**重算受影响元素的冲突：旧链上消失的相邻对所对应的冲突标记为失效（`active=0`、`invalidated_at` 落时间），其上的待审/已批准调整方案随即失效（不能再被审核）；邻接关系变化的镜头，旧锁定结论失效并重新解锁，邻接关系未变的镜头保留锁定。
- 整个调整（场景顺序、镜头状态/版本、元素冲突、失效标记、幂等记录）在单个事务内提交，失败回滚，不会留下半套变化。
- 写入失败后用**同一个 `request_id`** 重试：服务端识别为重复到达，原样返回首次结果（响应中 `retried=true`、`attempt` 为到达次数），不会重复执行；同一请求编号不能用于另一项操作。
- 页面显示当前叙事顺序（含每个镜头的锁定状态与版本号）、失效的调整方案，以及重试结果与 409 版本冲突提示。

## 主要接口

- `POST /api/users`、`POST /api/productions`
- `POST /api/productions/{id}/scenes`、`POST /api/scenes/{id}/shots`
- `POST /api/productions/{id}/elements`、`POST /api/elements/{id}/transitions`
- `POST /api/shots/{id}/states`、`POST /api/scenes/{id}/check`
- `POST /api/shots/{id}/reorder`（叙事位置调整：`narrative_order`、`expected_version`、`request_id`、`user_id`）
- `POST /api/conflicts/{id}/plans`、`POST /api/plans/{id}/review`
- `POST /api/conflicts/{id}/exemptions`
- `POST /api/shots/{id}/lock`
- `GET /api/productions/{id}/continuity`
